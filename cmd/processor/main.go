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
	"github.com/airbusgeo/geocube/interface/messaging/pubsub"
	"go.uber.org/zap"
)

type config struct {
	Project               string
	PsSubscription        string
	PsEventTopic          string
	WorkingDir            string
	StorageURI            string
	GeocubeServer         string
	GeocubeServerInsecure bool
	GeocubeServerApiKey   string
}

func newAppConfig() (*config, error) {
	project := flag.String("project", "", "subscription project (gcp only/not required in local usage)")
	psSubscription := flag.String("psSubscription", "", "pubsub tile subscription name")
	psEventTopic := flag.String("psEvent", "", "pubsub events topic name")
	workingDir := flag.String("workdir", "/local-ssd", "working directory to store intermediate results")
	storage := flag.String("storage-uri", "", "storage uri (currently supported: local, gs). To get outputs of the scene preprocessing graph and store outputs of the tile processing graph.")
	geocubeServer := flag.String("geocube-server", "127.0.0.1:8080", "address of geocube server")
	geocubeServerInsecure := flag.Bool("geocube-insecure", false, "connection to geocube server is insecure")
	geocubeServerApiKey := flag.String("geocube-apikey", "", "geocube server api key")
	flag.Parse()

	if *workingDir == "" {
		return nil, fmt.Errorf("missing workdir config flag")
	}
	if *storage == "" {
		return nil, fmt.Errorf("wrong storage-uri config flag")
	}
	if *geocubeServer == "" {
		return nil, fmt.Errorf("missing geocube server flag")
	}
	return &config{
		Project:               *project,
		PsSubscription:        *psSubscription,
		PsEventTopic:          *psEventTopic,
		WorkingDir:            *workingDir,
		StorageURI:            *storage,
		GeocubeServer:         *geocubeServer,
		GeocubeServerInsecure: *geocubeServerInsecure,
		GeocubeServerApiKey:   *geocubeServerApiKey,
	}, nil
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
		if config.PsSubscription != "" {
			logMessaging += fmt.Sprintf(" pulling on %s/%s", config.Project, config.PsSubscription)
			if jobConsumer, err = pubsub.NewConsumer(config.Project, config.PsSubscription); err != nil {
				return fmt.Errorf("pubsub.NewConsumer: %w", err)
			}
		}
		if config.PsEventTopic != "" {
			logMessaging += fmt.Sprintf(" pushing on %s/%s", config.Project, config.PsEventTopic)
			eventTopic, err := pubsub.NewPublisher(ctx, config.Project, config.PsEventTopic, pubsub.WithMaxRetries(5))
			if err != nil {
				return fmt.Errorf("messaging.NewPublisher: %w", err)
			}
			defer eventTopic.Stop()
			eventPublisher = eventTopic
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
		err := jobConsumer.Pull(ctx, func(ctx context.Context, msg *messaging.Message) (finalerr error) {
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
				if finalerr != nil {
					return
				}
				res := common.Result{
					Type:    common.ResultTypeTile,
					ID:      tile.ID,
					Status:  status,
					Message: message,
				}
				resb, err := json.Marshal(res)
				if err != nil {
					finalerr = service.MakeTemporary(fmt.Errorf("marshal: %w", err))
				} else if err := eventPublisher.Publish(ctx, resb); err != nil {
					finalerr = service.MakeTemporary(fmt.Errorf("failed to enqueue result: %w", err))
				}
			}()
			if err = processor.ProcessTile(ctx, storageService, gcclient, tile, config.WorkingDir); err != nil {
				if msg.TryCount >= maxTries {
					log.Logger(ctx).Error("failing job after too many retries", zap.Error(err))
					message = err.Error()
				} else if !service.Temporary(err) {
					if service.Fatal(err) {
						status = common.StatusFAILED
					}
					log.Logger(ctx).Error("job failed", zap.Error(err))
					message = err.Error()
				} else {
					log.Logger(ctx).Warn("job temporary failure", zap.Error(err))
					finalerr = err
				}
				return
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
