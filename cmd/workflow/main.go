package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"

	"github.com/airbusgeo/geocube-ingester/catalog"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/database/pg"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/airbusgeo/geocube-ingester/workflow"
	"github.com/airbusgeo/geocube/interface/messaging"
	"github.com/airbusgeo/geocube/interface/messaging/pubsub"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type autoscalerConfig struct {
	Namespace              string
	PsDownloaderQueue      string
	PsProcessorQueue       string
	DownloaderRC           string
	ProcessorRC            string
	MaxDownloaderInstances int64
	MaxProcessorInstances  int64
}

type catalogConfig struct {
	GeocubeServer         string
	GeocubeServerInsecure bool
	GeocubeServerApiKey   string
	ScihubUsername        string
	ScihubPassword        string
	GCStorage             string
}

type config struct {
	AppPort           string
	DbConnection      string
	PsProject         string
	PsSubscription    string
	PsDownloaderTopic string
	PsProcessorTopic  string
	AutoscalerConfig  autoscalerConfig
	CatalogConfig     catalogConfig
}

func newAppConfig() (*config, error) {
	appPort := flag.String("port", "8080", "workflow port ot use")
	dbConnection := flag.String("dbConnection", "", "database connection")
	psProject := flag.String("psProject", "", "pubsub subscription project (gcp only/not required in local usage)")
	psSubscription := flag.String("psSubscription", "", "pubsub event subscription name")
	psDownloaderTopic := flag.String("psDownloader-topic", "", "pubsub image-downloader topic name")
	psProcessorTopic := flag.String("psProcessor-topic", "", "pubsub tile-processor topic name")

	namespace := flag.String("namespace", "", "namespace (autoscaler)")
	downloaderRC := flag.String("downloader-rc", "", "image-downloader replication controller name (autoscaler)")
	processorRC := flag.String("processor-rc", "", "tile-processor replication controller name (autoscaler)")
	maxDownloaderInstances := flag.Int64("max-downloader", 10, "Max downloader instances (autoscaler)")
	maxProcessorInstances := flag.Int64("max-processor", 900, "Max Processor instances (autoscaler)")

	geocubeServer := flag.String("geocube-server", "127.0.0.1:8080", "address of geocube server")
	geocubeServerInsecure := flag.Bool("geocube-insecure", false, "connection to geocube server is insecure")
	geocubeServerApiKey := flag.String("geocube-apikey", "", "geocube server api key")
	scihubUsername := flag.String("scihub-username", "", "username to connect to the Scihub catalog service")
	scihubPassword := flag.String("scihub-password", "", "password to connect to the Scihub catalog service")
	gcstorage := flag.String("gcstorage", "", "GCS url where scenes are stored (for annotations) (optional)")
	flag.Parse()

	if *appPort == "" {
		return nil, fmt.Errorf("failed to initialize port application flag")
	}
	if *dbConnection == "" {
		return nil, fmt.Errorf("missing dbConnection config flag")
	}
	if *geocubeServer == "" {
		return nil, fmt.Errorf("missing geocube server flag")
	}
	return &config{
		AppPort:           *appPort,
		DbConnection:      *dbConnection,
		PsProject:         *psProject,
		PsSubscription:    *psSubscription,
		PsDownloaderTopic: *psDownloaderTopic,
		PsProcessorTopic:  *psProcessorTopic,
		AutoscalerConfig: autoscalerConfig{
			Namespace:              *namespace,
			PsDownloaderQueue:      *psDownloaderTopic,
			PsProcessorQueue:       *psProcessorTopic,
			DownloaderRC:           *downloaderRC,
			ProcessorRC:            *processorRC,
			MaxDownloaderInstances: *maxDownloaderInstances,
			MaxProcessorInstances:  *maxProcessorInstances,
		},
		CatalogConfig: catalogConfig{
			GeocubeServer:         *geocubeServer,
			GeocubeServerInsecure: *geocubeServerInsecure,
			GeocubeServerApiKey:   *geocubeServerApiKey,
			ScihubUsername:        *scihubUsername,
			ScihubPassword:        *scihubPassword,
			GCStorage:             *gcstorage,
		},
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

	// Start autoscalers
	if err = runAutoscalers(ctx, config.PsProject, config.AutoscalerConfig); err != nil {
		log.Logger(ctx).Warn("not running autoscalers", zap.Error(err))
	}

	// Connection to database
	db, err := pg.New(ctx, config.DbConnection)
	if err != nil {
		return fmt.Errorf("pg.New: %w", err)
	}

	// Messaging service
	var processorPublisher, downloaderPublisher messaging.Publisher
	var eventConsumer messaging.Consumer
	var logMessaging string
	{
		// Connection to pubsub
		if config.PsSubscription != "" {
			logMessaging += fmt.Sprintf(" pulling on %s/%s", config.PsProject, config.PsSubscription)
			eventConsumer, err = pubsub.NewConsumer(config.PsProject, config.PsSubscription)
			if err != nil {
				return fmt.Errorf("pubsub.new: %w", err)
			}
		}

		if config.PsDownloaderTopic != "" {
			logMessaging += fmt.Sprintf(" pushing downloaderJobs on %s/%s", config.PsProject, config.PsDownloaderTopic)
			publisher, err := pubsub.NewPublisher(ctx, config.PsProject, config.PsDownloaderTopic)
			if err != nil {
				return fmt.Errorf("pubsub.NewPublisher(Downloader): %w", err)
			}
			defer publisher.Stop()
			downloaderPublisher = publisher
		}

		if config.PsProcessorTopic != "" {
			logMessaging += fmt.Sprintf(" pushing processorJobs on %s/%s", config.PsProject, config.PsProcessorTopic)
			publisher, err := pubsub.NewPublisher(ctx, config.PsProject, config.PsProcessorTopic)
			if err != nil {
				return fmt.Errorf("pubsub.NewPublisher(Processor): %w", err)
			}
			defer publisher.Stop()
			processorPublisher = publisher
		}
	}
	if eventConsumer == nil {
		return fmt.Errorf("missing configuration for messaging.EventConsumer")
	}
	if processorPublisher == nil {
		return fmt.Errorf("missing configuration for messaging.ProcessorPublisher")
	}
	if downloaderPublisher == nil {
		return fmt.Errorf("missing configuration for messaging.DownloaderPublisher")
	}

	catalog := catalog.Catalog{}
	{
		// Geocube client
		if config.CatalogConfig.GeocubeServer != "" {
			var tlsConfig *tls.Config
			if !config.CatalogConfig.GeocubeServerInsecure {
				tlsConfig = &tls.Config{}
			}
			if catalog.GeocubeClient, err = service.NewGeocubeClient(ctx, config.CatalogConfig.GeocubeServer, config.CatalogConfig.GeocubeServerApiKey, tlsConfig); err != nil {
				return fmt.Errorf("connection to geocube: %w", err)
			}
		} else {
			log.Logger(ctx).Warn("Geocube server is not configured. Some catalogue functions are disabled.")
		}

		// Connection to the external catalogue service
		// Scihub connection
		catalog.ScihubUser = config.CatalogConfig.ScihubUsername
		catalog.ScihubPword = config.CatalogConfig.ScihubPassword
		// GCStorage
		catalog.GCSAnnotationsBucket = config.CatalogConfig.GCStorage
	}

	// Create Workflow Server
	wf := workflow.NewWorkflow(db, downloaderPublisher, processorPublisher, &catalog)
	// New handler
	router := wf.NewHandler()
	wf.CatalogHandler(router.(*mux.Router))
	headersOk := handlers.AllowedHeaders([]string{"*"})
	originsOk := handlers.AllowedOrigins([]string{"*"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "POST", "PUT", "OPTIONS"})
	s := http.Server{
		Addr:    ":" + config.AppPort,
		Handler: handlers.CORS(originsOk, headersOk, methodsOk)(router),
	}
	go func() {
		if err := s.ListenAndServe(); err != nil {
			log.Logger(ctx).Error(err.Error())
		}
	}()

	log.Logger(ctx).Debug("workflow starts" + logMessaging)
	for {
		err := eventConsumer.Pull(ctx, func(ctx context.Context, msg *messaging.Message) error {
			ctx = log.With(ctx, "msgID", msg.ID)
			log.Logger(log.With(ctx, "body", string(msg.Data))).Sugar().Debugf("message %s try %d", msg.ID, msg.TryCount)
			if msg.TryCount > 30 {
				return fmt.Errorf("bailing out after too many retries")
			}
			result := common.Result{}
			if err := json.Unmarshal(msg.Data, &result); err != nil {
				return fmt.Errorf("invalid payload: %w", err)
			} else if (result.Type != common.ResultTypeTile && result.Type != common.ResultTypeScene) || result.ID == 0 {
				return fmt.Errorf("invalid payload %s %d", result.Type, result.ID)
			}
			if err := wf.ResultHandler(ctx, result); err != nil {
				return service.MakeTemporary(fmt.Errorf("failed to process %s %d: %w", result.Type, result.ID, err))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("ps.process: %w", err)
		}
	}
}
