package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/database/pg"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/airbusgeo/geocube-ingester/workflow"
	"github.com/airbusgeo/geocube/interface/autoscaler/qbas"
	"github.com/airbusgeo/geocube/interface/messaging"
	"github.com/airbusgeo/geocube/interface/messaging/pgqueue"
	"github.com/airbusgeo/geocube/interface/messaging/pubsub"
	"github.com/gorilla/handlers"
	"go.uber.org/zap"
)

type autoscalerConfig struct {
	Namespace              string
	DownloaderRC           string
	ProcessorRC            string
	MaxDownloaderInstances int64
	MaxProcessorInstances  int64
}

type catalogConfig struct {
	GeocubeServer                  string
	GeocubeServerInsecure          bool
	GeocubeServerApiKey            string
	AnnotationsURLs                []string
	OneAtlasUsername               string
	OneAtlasApikey                 string
	OneAtlasEndpoint               string
	OneAtlasOrderEndpoint          string
	OneAtlasAuthenticationEndpoint string
	CopernicusCatalog              bool
	CreodiasCatalog                bool
}

type config struct {
	AppPort          string
	TLS              bool
	BearerAuth       string
	DbConnection     string
	PgqDbConnection  string
	PsProject        string
	EventQueue       string
	DownloaderQueue  string
	ProcessorQueue   string
	AutoscalerConfig autoscalerConfig
	CatalogConfig    catalogConfig
}

func newAppConfig() (*config, error) {
	var annotationsURLs string
	config := config{}
	flag.StringVar(&config.AppPort, "port", "8080", "workflow port ot use")
	flag.BoolVar(&config.TLS, "tls", false, "enable TLS protocol (certificate and key must be /tls/tls.crt and /tls/tls.key)")
	flag.StringVar(&config.BearerAuth, "bearer-auth", "", "bearer authentication (token) (optional)")

	// Database
	flag.StringVar(&config.DbConnection, "db-connection", "", "database connection")

	// Messaging
	flag.StringVar(&config.PgqDbConnection, "pgq-connection", "", "enable pgq messaging system with a connection to the database")
	flag.StringVar(&config.PsProject, "ps-project", "", "pubsub subscription project (gcp only/not required in local usage)")
	flag.StringVar(&config.EventQueue, "event-queue", "", "name of the queue for job events (pgqueue or pubsub subscription)")
	flag.StringVar(&config.DownloaderQueue, "downloader-queue", "", "name of the queue for downloader jobs (pgqueue or pubsub topic)")
	flag.StringVar(&config.ProcessorQueue, "processor-queue", "", "name of the queue for processor jobs (pgqueue or pubsub topic)")

	// Autoscaller
	flag.StringVar(&config.AutoscalerConfig.Namespace, "namespace", "", "namespace (autoscaler)")
	flag.StringVar(&config.AutoscalerConfig.DownloaderRC, "downloader-rc", "", "image-downloader replication controller name (autoscaler)")
	flag.StringVar(&config.AutoscalerConfig.ProcessorRC, "processor-rc", "", "tile-processor replication controller name (autoscaler)")
	flag.Int64Var(&config.AutoscalerConfig.MaxDownloaderInstances, "max-downloader", 10, "Max downloader instances (autoscaler)")
	flag.Int64Var(&config.AutoscalerConfig.MaxProcessorInstances, "max-processor", 900, "Max Processor instances (autoscaler)")

	// Geocube
	flag.StringVar(&config.CatalogConfig.GeocubeServer, "geocube-server", "127.0.0.1:8080", "address of geocube server")
	flag.BoolVar(&config.CatalogConfig.GeocubeServerInsecure, "geocube-insecure", false, "connection to geocube server is insecure")
	flag.StringVar(&config.CatalogConfig.GeocubeServerApiKey, "geocube-apikey", "", "geocube server api key")

	// Providers
	flag.StringVar(&annotationsURLs, "annotations-urls", "", "URL (local/gs/aws) containing S1-scenes (as zip) to read annotations without downloading the whole file (optional, contains identifiers between brackets that will be replaced by those of the scene. E.g: gs://bucket/{DATE}/{SCENE}.zip), several urls are coma separated")
	flag.StringVar(&config.CatalogConfig.OneAtlasUsername, "oneatlas-username", "", "oneatlas account username (optional). To configure Oneatlas as a potential image Provider.")
	flag.StringVar(&config.CatalogConfig.OneAtlasApikey, "oneatlas-apikey", "", "oneatlas account apikey (to generate an api key for your account: https://account.foundation.oneatlas.airbus.com/api-keys)")
	flag.StringVar(&config.CatalogConfig.OneAtlasEndpoint, "oneatlas-endpoint", "https://search.foundation.api.oneatlas.airbus.com/api/v2/opensearch", "oneatlas endpoint to search products from the catalogue")
	flag.StringVar(&config.CatalogConfig.OneAtlasOrderEndpoint, "oneatlas-order-endpoint", "https://data.api.oneatlas.airbus.com", "oneatlas order endpoint to estimate processing price")
	flag.StringVar(&config.CatalogConfig.OneAtlasAuthenticationEndpoint, "oneatlas-auth-endpoint", "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token", "oneatlas order endpoint to use")
	flag.BoolVar(&config.CatalogConfig.CopernicusCatalog, "copernicus-catalog", false, "Use the Copernicus catalog service (search data)")
	flag.BoolVar(&config.CatalogConfig.CreodiasCatalog, "creodias-catalog", false, "Use the creodias catalog service (search data)")

	flag.Parse()

	if config.AppPort == "" {
		return nil, fmt.Errorf("failed to initialize port application flag")
	}
	if config.DbConnection == "" {
		return nil, fmt.Errorf("missing dbConnection config flag")
	}
	if len(annotationsURLs) > 0 {
		config.CatalogConfig.AnnotationsURLs = strings.Split(annotationsURLs, ",")
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

	// Connection to database
	db, err := pg.New(ctx, config.DbConnection)
	if err != nil {
		return fmt.Errorf("pg.New: %w", err)
	}

	// Messaging service
	var processorPublisher, downloaderPublisher messaging.Publisher
	var processorQueue, downloaderQueue qbas.Queue
	var eventConsumer messaging.Consumer
	var logMessaging string
	{
		if config.PgqDbConnection != "" {
			// Connection to pgqueue
			db, w, err := pgqueue.SqlConnect(ctx, config.PgqDbConnection)
			if err != nil {
				return fmt.Errorf("MessagingService: %w", err)

			}
			if config.EventQueue != "" {
				logMessaging += fmt.Sprintf(" pulling on pgqueue:%s", config.EventQueue)
				consumer := pgqueue.NewConsumer(db, config.EventQueue)
				defer consumer.Stop()
				eventConsumer = consumer
			}

			if config.DownloaderQueue != "" {
				logMessaging += fmt.Sprintf(" pushing downloaderJobs on pgqueue:%s", config.DownloaderQueue)
				downloaderPublisher = pgqueue.NewPublisher(w, config.DownloaderQueue, pgqueue.WithMaxRetries(5),
					pgqueue.WithJobRetryWaits([]time.Duration{10 * time.Minute, 10 * time.Minute, 10 * time.Minute, 10 * time.Minute, 10 * time.Minute, 10 * time.Minute, 10 * time.Minute, 10 * time.Minute, 10 * time.Minute}))
				downloaderQueue = pgqueue.NewConsumer(db, config.DownloaderQueue)
			}

			if config.ProcessorQueue != "" {
				logMessaging += fmt.Sprintf(" pushing processorJobs on pgqueue:%s", config.ProcessorQueue)
				processorPublisher = pgqueue.NewPublisher(w, config.ProcessorQueue, pgqueue.WithMaxRetries(5))
				processorQueue = pgqueue.NewConsumer(db, config.ProcessorQueue)
			}
		} else {
			// Connection to pubsub
			if config.EventQueue != "" {
				logMessaging += fmt.Sprintf(" pulling on %s/%s", config.PsProject, config.EventQueue)
				if eventConsumer, err = pubsub.NewConsumer(config.PsProject, config.EventQueue); err != nil {
					return fmt.Errorf("pubsub.NewConsumer(Event): %w", err)
				}
			}

			if config.DownloaderQueue != "" {
				logMessaging += fmt.Sprintf(" pushing downloaderJobs on %s/%s", config.PsProject, config.DownloaderQueue)
				publisher, err := pubsub.NewPublisher(ctx, config.PsProject, config.DownloaderQueue)
				if err != nil {
					return fmt.Errorf("pubsub.NewPublisher(Downloader): %w", err)
				}
				defer publisher.Stop()
				downloaderPublisher = publisher
				if downloaderQueue, err = pubsub.NewConsumer(config.PsProject, config.DownloaderQueue); err != nil {
					return fmt.Errorf("pubsub.NewConsumer(Downloader): %w", err)
				}
			}

			if config.ProcessorQueue != "" {
				logMessaging += fmt.Sprintf(" pushing processorJobs on %s/%s", config.PsProject, config.ProcessorQueue)
				publisher, err := pubsub.NewPublisher(ctx, config.PsProject, config.ProcessorQueue)
				if err != nil {
					return fmt.Errorf("pubsub.NewPublisher(Processor): %w", err)
				}
				defer publisher.Stop()
				processorPublisher = publisher
				if processorQueue, err = pubsub.NewConsumer(config.PsProject, config.ProcessorQueue); err != nil {
					return fmt.Errorf("pubsub.NewConsumer(Processor): %w", err)
				}
			}
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
	if downloaderQueue == nil {
		return fmt.Errorf("missing configuration for messaging.DownloaderQueue")
	}
	if processorQueue == nil {
		return fmt.Errorf("missing configuration for messaging.ProcessorQueue")
	}

	// Autoscaler
	if err = runAutoscalers(ctx, downloaderQueue, processorQueue, config.AutoscalerConfig); err != nil {
		log.Logger(ctx).Warn("not running autoscalers", zap.Error(err))
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
		// GCStorage
		catalog.AnnotationsURLs = config.CatalogConfig.AnnotationsURLs

		// Copernicus Catalogue
		catalog.CopernicusCatalog = config.CatalogConfig.CopernicusCatalog

		// Creodias Catalogue
		catalog.CreodiasCatalog = config.CatalogConfig.CreodiasCatalog

		// OneAtlas
		catalog.OneAtlasCatalogUser = config.CatalogConfig.OneAtlasUsername
		catalog.OneAtlasApikey = config.CatalogConfig.OneAtlasApikey
		catalog.OneAtlasCatalogEndpoint = config.CatalogConfig.OneAtlasEndpoint
		catalog.OneAtlasOrderEndpoint = config.CatalogConfig.OneAtlasOrderEndpoint
		catalog.OneAtlasAuthenticationEndpoint = config.CatalogConfig.OneAtlasAuthenticationEndpoint
	}

	// Create Workflow Server
	wf := workflow.NewWorkflow(db, downloaderPublisher, processorPublisher, &catalog)
	// New handler
	router := wf.NewRouter()
	catalog.Workflow = wf
	catalog.AddHandler(router)
	headersOk := handlers.AllowedHeaders([]string{"*", AuthorizationHeader})
	originsOk := handlers.AllowedOrigins([]string{"*"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "POST", "PUT", "OPTIONS", "DELETE"})

	// Authentication
	bearerAuths = map[string]string{"default": config.BearerAuth}
	router.Use(BearerAuthenticate)

	s := http.Server{
		Addr:    ":" + config.AppPort,
		Handler: handlers.CORS(originsOk, headersOk, methodsOk)(router),
	}
	logMessaging += " listening on " + config.AppPort
	go func() {
		if config.TLS {
			err = s.ListenAndServeTLS("/tls/tls.crt", "/tls/tls.key")
		} else {
			err = s.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Logger(ctx).Fatal("srv.ListenAndServe", zap.Error(err))
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
