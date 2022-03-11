package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/processor"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/airbusgeo/geocube/interface/messaging"
	"github.com/airbusgeo/geocube/interface/messaging/pgqueue"
	"github.com/airbusgeo/geocube/interface/messaging/pubsub"
	"go.uber.org/zap"
)

type config struct {
	WorkingDir string
	StorageURI string

	PgqDbConnection string
	PsProject       string
	JobQueue        string
	EventQueue      string

	GeocubeServer         string
	GeocubeServerInsecure bool
	GeocubeServerApiKey   string
}

func newAppConfig() (*config, error) {
	config := config{}
	// Global config
	flag.StringVar(&config.WorkingDir, "workdir", "/local-ssd", "working directory to store intermediate results")
	flag.StringVar(&config.StorageURI, "storage-uri", "", "storage uri (currently supported: local, gs). To get outputs of the scene preprocessing graph and store outputs of the tile processing graph.")

	// Messaging
	flag.StringVar(&config.PgqDbConnection, "pgq-connection", "", "enable pgq messaging system with a connection to the database")
	flag.StringVar(&config.PsProject, "ps-project", "", "pubsub subscription project (gcp only/not required in local usage)")
	flag.StringVar(&config.JobQueue, "job-queue", "", "name of the queue for processor jobs (pgqueue or pubsub subscription)")
	flag.StringVar(&config.EventQueue, "event-queue", "", "name of the queue for job events (pgqueue or pubsub topic)")

	// Geocube connection
	flag.StringVar(&config.GeocubeServer, "geocube-server", "127.0.0.1:8080", "address of geocube server")
	flag.BoolVar(&config.GeocubeServerInsecure, "geocube-insecure", false, "connection to geocube server is insecure")
	flag.StringVar(&config.GeocubeServerApiKey, "geocube-apikey", "", "geocube server api key")
	flag.Parse()

	if config.WorkingDir == "" {
		return nil, fmt.Errorf("missing workdir config flag")
	}
	if config.StorageURI == "" {
		return nil, fmt.Errorf("wrong storage-uri config flag")
	}
	if config.GeocubeServer == "" {
		return nil, fmt.Errorf("missing geocube server flag")
	}
	return &config, nil
}

func main() {
	ctx := context.Background()
	err := run(ctx)
	if err != nil {
		log.Fatal("error", zap.Error(err))
	}
}

func run(ctx context.Context) error {
	config, err := newAppConfig()
	if err != nil {
		return err
	}

	var eventPublisher messaging.Publisher
	var jobConsumer messaging.Consumer
	var logMessaging string
	{
		if config.PgqDbConnection != "" {
			db, w, err := pgqueue.SqlConnect(ctx, config.PgqDbConnection)
			if err != nil {
				return fmt.Errorf("MessagingService: %w", err)
			}
			if config.JobQueue != "" {
				logMessaging += fmt.Sprintf(" pulling on pgqueue:%s", config.JobQueue)
				consumer := pgqueue.NewConsumer(db, config.JobQueue)
				defer consumer.Stop()
				jobConsumer = consumer
			}
			if config.EventQueue != "" {
				logMessaging += fmt.Sprintf(" pushing on pgqueue:%s", config.EventQueue)
				eventPublisher = pgqueue.NewPublisher(w, config.EventQueue, pgqueue.WithMaxRetries(5))
			}
		} else if config.PsProject != "" {
			if config.JobQueue != "" {
				logMessaging += fmt.Sprintf(" pulling on %s/%s", config.PsProject, config.JobQueue)
				if jobConsumer, err = pubsub.NewConsumer(config.PsProject, config.JobQueue); err != nil {
					return fmt.Errorf("pubsub.NewConsumer: %w", err)
				}
			}
			if config.EventQueue != "" {
				logMessaging += fmt.Sprintf(" pushing on %s/%s", config.PsProject, config.EventQueue)
				eventTopic, err := pubsub.NewPublisher(ctx, config.PsProject, config.EventQueue, pubsub.WithMaxRetries(5))
				if err != nil {
					return fmt.Errorf("messaging.NewPublisher: %w", err)
				}
				defer eventTopic.Stop()
				eventPublisher = eventTopic
			}
		}
	}
	if jobConsumer == nil {
		return fmt.Errorf("missing configuration for messaging.JobConsumer")
	}
	if eventPublisher == nil {
		return fmt.Errorf("missing configuration for messaging.EventPublisher")
	}

	storageService, err := service.NewStorageStrategy(ctx, config.StorageURI)
	if err != nil {
		return fmt.Errorf("storage[%s].%w", config.StorageURI, err)
	}

	// Geocube client
	var tlsConfig *tls.Config
	if !config.GeocubeServerInsecure {
		tlsConfig = &tls.Config{}
	}
	gcclient, err := service.NewGeocubeClient(ctx, config.GeocubeServer, config.GeocubeServerApiKey, tlsConfig)
	if err != nil {
		return err
	}

	jobStarted := time.Time{}
	go func() {
		http.HandleFunc("/termination_cost", func(w http.ResponseWriter, r *http.Request) {
			terminationCost := 0
			if jobStarted != (time.Time{}) {
				terminationCost = int(time.Since(jobStarted).Seconds() * 1000) //milliseconds since task was leased
			}
			fmt.Fprintf(w, "%d", terminationCost)
		})
		http.ListenAndServe(":9000", nil)
	}()

	maxTries := 15 //Must be less than the configured number of tries of the pubsub topic

	log.Logger(ctx).Debug("processor starts" + logMessaging)
	for {
		err := jobConsumer.Pull(ctx, func(ctx context.Context, msg *messaging.Message) (err error) {
			jobStarted = time.Now()
			defer func() {
				jobStarted = time.Time{}
			}()
			ctx = log.With(ctx, "msgID", msg.ID)
			log.Logger(log.With(ctx, "body", string(msg.Data))).Sugar().Debugf("message %s try %d", msg.ID, msg.TryCount)
			status := common.StatusRETRY
			tile := common.TileToProcess{}
			message := ""
			if err := json.Unmarshal(msg.Data, &tile); err != nil {
				return fmt.Errorf("invalid payload: %w", err)
			} else if tile.ID == 0 {
				return fmt.Errorf("invalid payload: %d", tile.ID)
			}

			defer func() {
				if err != nil && service.Temporary(err) {
					log.Logger(ctx).Warn("job temporary failure", zap.Error(err))
					return
				}
				if err != nil {
					log.Logger(ctx).Warn("job failed", zap.Error(err))
					message = err.Error()
				}
				res := common.Result{
					Type:    common.ResultTypeTile,
					ID:      tile.ID,
					Status:  status,
					Message: message,
				}
				resb, e := json.Marshal(res)
				if e != nil {
					err = service.MakeTemporary(fmt.Errorf("marshal: %w", e))
				} else if e := eventPublisher.Publish(ctx, resb); e != nil {
					err = service.MakeTemporary(fmt.Errorf("failed to enqueue result: %w", e))
				}
			}()
			if msg.TryCount > maxTries {
				return fmt.Errorf("too many retries")
			}

			if err = processor.ProcessTile(ctx, storageService, gcclient, tile, config.WorkingDir); err != nil {
				if msg.TryCount >= maxTries {
					return fmt.Errorf("too many retries: %w", err)
				}
				if service.Fatal(err) {
					status = common.StatusFAILED
				}
				return err
			}
			log.Logger(ctx).Sugar().Infof("successfully processed tile %s/%s", tile.Scene.SourceID, tile.SourceID)
			status = common.StatusDONE
			return
		})
		if err != nil {
			return fmt.Errorf("ps.process: %w", err)
		}
	}
}
