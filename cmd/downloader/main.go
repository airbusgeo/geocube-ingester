package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/downloader"
	"github.com/airbusgeo/geocube-ingester/graph"
	"github.com/airbusgeo/geocube-ingester/interface/provider"
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

	PsProject       string
	JobQueue        string
	EventQueue      string
	PgqDbConnection string

	LocalProviderPath              string
	PepsUsername                   string
	PepsPassword                   string
	OndaUsername                   string
	OndaPassword                   string
	ASFToken                       string
	ScihubUsername                 string
	ScihubPassword                 string
	CreodiasUsername               string
	CreodiasPassword               string
	OneAtlasUsername               string
	OneAtlasDownloadEndpoint       string
	OneAtlasOrderEndpoint          string
	OneAtlasApikey                 string
	OneAtlasAuthenticationEndpoint string
	SoblooApiKey                   string
	MundiSeeedToken                string
	GSProviderBuckets              []string

	WithDockerEngine bool
	Docker           graph.DockerConfig
}

func newAppConfig() (*config, error) {
	config := config{}
	// Global config
	flag.StringVar(&config.WorkingDir, "workdir", "/local-ssd", "working directory to store intermediate results")
	flag.StringVar(&config.StorageURI, "storage-uri", "", "storage uri (currently supported: local, gs). To store outputs of the scene preprocessing graph.")

	// Messaging
	flag.StringVar(&config.PgqDbConnection, "pgq-connection", "", "enable pgq messaging system with a connection to the database")
	flag.StringVar(&config.PsProject, "ps-project", "", "pubsub subscription project (gcp only/not required in local usage)")
	flag.StringVar(&config.JobQueue, "job-queue", "", "name of the queue for downloader jobs (pgqueue or pubsub subscription)")
	flag.StringVar(&config.EventQueue, "event-queue", "", "name of the queue for job events (pgqueue or pubsub topic)")

	// Providers
	flag.StringVar(&config.LocalProviderPath, "local-path", "", "local path where images are stored (optional). To configure a local path as a potential image Provider.")
	flag.StringVar(&config.PepsUsername, "peps-username", "", "peps account username (optional). To configure PEPS as a potential image Provider.")
	flag.StringVar(&config.PepsPassword, "peps-password", "", "peps account password (optional)")
	flag.StringVar(&config.OndaUsername, "onda-username", "", "onda account username (optional). To configure ONDA as a potential image Provider.")
	flag.StringVar(&config.OndaPassword, "onda-password", "", "onda account password (optional)")
	flag.StringVar(&config.ASFToken, "asf-token", "", "ASF token (optional). To configure Alaska Satellite Facility as a potential image Provider.")
	flag.StringVar(&config.ScihubUsername, "scihub-username", "", "scihub account username (optional). To configure Scihub as a potential image Provider.")
	flag.StringVar(&config.ScihubPassword, "scihub-password", "", "scihub account password (optional)")
	flag.StringVar(&config.CreodiasUsername, "creodias-username", "", "creodias account username (optional). To configure Creodias as a potential image Provider.")
	flag.StringVar(&config.CreodiasPassword, "creodias-password", "", "creodias account password (optional)")
	flag.StringVar(&config.SoblooApiKey, "sobloo-apikey", "", "sobloo api-key (optional). To configure Sobloo as a potential image Provider.")
	flag.StringVar(&config.MundiSeeedToken, "mundi-seeed-token", "", "mundi seeed-token (optional). To configure Mundi as a potential image Provider.")
	flag.StringVar(&config.OneAtlasUsername, "oneatlas-username", "", "oneatlas account username (optional). To configure Oneatlas as a potential image Provider.")
	flag.StringVar(&config.OneAtlasApikey, "oneatlas-apikey", "", "oneatlas apikey to use")
	flag.StringVar(&config.OneAtlasDownloadEndpoint, "oneatlas-download-endpoint", "https://access.foundation.api.oneatlas.airbus.com/api/v1/items", "oneatlas download endpoint to use")
	flag.StringVar(&config.OneAtlasOrderEndpoint, "oneatlas-order-endpoint", "https://data.api.oneatlas.airbus.com", "oneatlas order endpoint to use")
	flag.StringVar(&config.OneAtlasAuthenticationEndpoint, "oneatlas-auth-endpoint", "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token", "oneatlas order endpoint to use")
	gsProviderBuckets := flag.String("gs-provider-buckets", "", `Google Storage buckets. List of constellation:bucket comma-separated (optional). To configure GS as a potential image Provider.
	bucket can contain several {IDENTIFIER} than will be replaced according to the sceneName.
	IDENTIFIER must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)
	 `)

	// Docker processing Images connection
	flag.BoolVar(&config.WithDockerEngine, "with-docker-engine", false, "activate the support of graph.engine == 'docker' (require a running docker-daemon)")
	dockerEnvsStr := config.Docker.SetFlags()

	flag.Parse()

	if *dockerEnvsStr != "" {
		config.Docker.Envs = strings.Split(*dockerEnvsStr, ",")
	}

	if config.WorkingDir == "" {
		return nil, fmt.Errorf("missing workdir config flag")
	}
	if config.StorageURI == "" {
		return nil, fmt.Errorf("missing storage-uri config flag")
	}
	if *gsProviderBuckets != "" {
		config.GSProviderBuckets = strings.Split(*gsProviderBuckets, ",")
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

	var jobConsumer messaging.Consumer
	var eventPublisher messaging.Publisher
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
		} else {
			if config.JobQueue != "" {
				logMessaging += fmt.Sprintf(" pulling on pubsub:%s/%s", config.PsProject, config.JobQueue)
				if jobConsumer, err = pubsub.NewConsumer(config.PsProject, config.JobQueue); err != nil {
					return fmt.Errorf("pubsub.NewConsumer: %w", err)
				}
			}
			if config.EventQueue != "" {
				logMessaging += fmt.Sprintf(" pushing on pubsub:%s/%s", config.PsProject, config.EventQueue)
				eventTopic, err := pubsub.NewPublisher(ctx, config.PsProject, config.EventQueue, pubsub.WithMaxRetries(5))
				if err != nil {
					return fmt.Errorf("pubsub.NewPublisher: %w", err)
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
		return fmt.Errorf("storage %s: %w"+config.StorageURI, err)
	}

	// Load image providers
	var imageProviders []provider.ImageProvider
	var providerNames []string
	if config.LocalProviderPath != "" {
		providerNames = append(providerNames, "local ("+config.LocalProviderPath+")")
		imageProviders = append(imageProviders, provider.NewLocalImageProvider(config.LocalProviderPath))

	}
	if len(config.GSProviderBuckets) != 0 {
		gs := provider.NewGSImageProvider()
		for _, gsbucket := range config.GSProviderBuckets {
			bucket := strings.SplitN(gsbucket, ":", 2)
			if len(bucket) != 2 {
				return fmt.Errorf("malformed GSBuckets config. Must be constellation:bucket")
			}
			if err := gs.AddBucket(bucket[0], bucket[1]); err != nil {
				return fmt.Errorf("malformed GSBuckets config. Must be constellation:bucket")
			}
		}
		providerNames = append(providerNames, "GS ("+strings.Join(config.GSProviderBuckets, ", ")+")")
		imageProviders = append(imageProviders, gs)
	}
	if config.SoblooApiKey != "" {
		providerNames = append(providerNames, "Sobloo")
		imageProviders = append(imageProviders, provider.NewSoblooImageProvider(config.SoblooApiKey))
	}
	if config.PepsUsername != "" {
		providerNames = append(providerNames, "PEPS ("+config.PepsUsername+")")
		imageProviders = append(imageProviders, provider.NewPEPSDiasImageProvider(config.PepsUsername, config.PepsPassword))
	}
	if config.OndaUsername != "" {
		providerNames = append(providerNames, "Onda ("+config.OndaUsername+")")
		imageProviders = append(imageProviders, provider.NewONDADiasImageProvider(config.OndaUsername, config.OndaPassword))
	}
	if config.ASFToken != "" {
		providerNames = append(providerNames, "ASF ("+config.ASFToken+")")
		imageProviders = append(imageProviders, provider.NewASFImageProvider(config.ASFToken))
	}
	if config.ScihubUsername != "" {
		providerNames = append(providerNames, "Scihub ("+config.ScihubUsername+")")
		imageProviders = append(imageProviders, provider.NewScihubImageProvider(config.ScihubUsername, config.ScihubPassword))
	}
	if config.MundiSeeedToken != "" {
		providerNames = append(providerNames, "Mundi")
		imageProviders = append(imageProviders, provider.NewMundiImageProvider(config.MundiSeeedToken))
	}
	if config.CreodiasUsername != "" {
		providerNames = append(providerNames, "Creodias ("+config.CreodiasUsername+")")
		imageProviders = append(imageProviders, provider.NewCreoDiasImageProvider(config.CreodiasUsername, config.CreodiasPassword))
	}
	if config.OneAtlasUsername != "" {
		providerNames = append(providerNames, "OneAtlas")
		imageProviders = append(imageProviders, provider.NewOneAtlasProvider(ctx,
			config.OneAtlasUsername,
			config.OneAtlasApikey,
			config.OneAtlasDownloadEndpoint,
			config.OneAtlasOrderEndpoint,
			config.OneAtlasAuthenticationEndpoint,
		))
	}
	if len(imageProviders) == 0 {
		return fmt.Errorf("no image providers defined... ")
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

	var dockerManager graph.DockerManager
	if config.WithDockerEngine {
		dockerManager, err = graph.NewDockerManager(ctx, config.Docker)
		if err != nil {
			return err
		}
	}

	maxTries := 15
	log.Logger(ctx).Debug("downloader starts " + logMessaging + " downloading image from " + strings.Join(providerNames, ", ") + " exporting to " + config.StorageURI)
	for {
		err := jobConsumer.Pull(ctx, func(ctx context.Context, msg *messaging.Message) (err error) {
			jobStarted = time.Now()
			defer func() {
				jobStarted = time.Time{}
			}()
			ctx = log.With(ctx, "msgID", msg.ID)
			log.Logger(log.With(ctx, "body", string(msg.Data))).Sugar().Debugf("message %s try %d", msg.ID, msg.TryCount)
			status := common.StatusRETRY
			scene := common.Scene{}
			message := ""

			if err := json.Unmarshal(msg.Data, &scene); err != nil {
				return fmt.Errorf("invalid payload: %w", err)
			} else if scene.ID == 0 || len(scene.Data.TileMappings) == 0 {
				return fmt.Errorf("invalid payload : %d-%d", scene.ID, len(scene.Data.TileMappings))
			}

			ctx = log.With(ctx, "image", scene.SourceID)

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
					Type:    common.ResultTypeScene,
					ID:      scene.ID,
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

			if err := downloader.ProcessScene(ctx, imageProviders, storageService, dockerManager, scene, config.WorkingDir); err != nil {
				if msg.TryCount >= maxTries {
					return fmt.Errorf("too many retries: %w", err)
				}
				return err
			}
			log.Logger(ctx).Sugar().Infof("successfully processed scene %s", scene.SourceID)
			status = common.StatusDONE
			return
		})
		if err != nil {
			return fmt.Errorf("ps.process: %w", err)
		}
	}
}
