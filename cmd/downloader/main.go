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
	"github.com/airbusgeo/geocube-ingester/interface/provider"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/airbusgeo/geocube/interface/messaging"
	"github.com/airbusgeo/geocube/interface/messaging/pubsub"
	"go.uber.org/zap"
)

type config struct {
	Project           string
	PsSubscription    string
	PsEventTopic      string
	WorkingDir        string
	StorageURI        string
	LocalProviderPath string
	PepsUsername      string
	PepsPassword      string
	OndaUsername      string
	OndaPassword      string
	ScihubUsername    string
	ScihubPassword    string
	CreodiasUsername  string
	CreodiasPassword  string
	SoblooApiKey      string
	MundiSeeedToken   string
	GSProviderBuckets []string
}

func newAppConfig() (*config, error) {
	project := flag.String("project", "", "subscription project (gcp only/not required in local usage)")
	psSubscription := flag.String("psSubscription", "", "pubsub image subscription name")
	psEventTopic := flag.String("psEvent", "", "pubsub events topic name")
	workingDir := flag.String("workdir", "/local-ssd", "working directory to store intermediate results")
	storageURI := flag.String("storage-uri", "", "storage uri (currently supported: local, gs). To store outputs of the scene preprocessing graph.")
	localProviderPath := flag.String("local-path", "", "local path where images are stored (optional). To configure a local path as a potential image Provider.")
	pepsUsername := flag.String("peps-username", "", "peps account username (optional). To configure PEPS as a potential image Provider.")
	pepsPassword := flag.String("peps-password", "", "peps account password (optional)")
	ondaUsername := flag.String("onda-username", "", "onda account username (optional). To configure ONDA as a potential image Provider.")
	ondaPassword := flag.String("onda-password", "", "onda account password (optional)")
	scihubUsername := flag.String("scihub-username", "", "scihub account username (optional). To configure Scihub as a potential image Provider.")
	scihubPassword := flag.String("scihub-password", "", "scihub account password (optional)")
	creodiasUsername := flag.String("creodias-username", "", "creodias account username (optional). To configure Creodias as a potential image Provider.")
	creodiasPassword := flag.String("creodias-password", "", "creodias account password (optional)")
	soblooApiKey := flag.String("sobloo-apikey", "", "sobloo api-key (optional). To configure Sobloo as a potential image Provider.")
	mundiSeeedToken := flag.String("mundi-seeed-token", "", "mundi seeed-token (optional). To configure Mundi as a potential image Provider.")
	gsProviderBuckets := flag.String("gs-provider-buckets", "", `Google Storage buckets. List of constellation:bucket comma-separated (optional). To configure GS as a potential image Provider.
	bucket can contain several {IDENTIFIER} than will be replaced according to the sceneName.
	IDENTIFIER must be one of SCENE, MISSION_ID, PRODUCT_LEVEL, DATE(YEAR/MONTH/DAY), TIME(HOUR/MINUTE/SECOND), PDGS, ORBIT, TILE (LATITUDE_BAND/GRID_SQUARE/GRANULE_ID)
	 `)
	flag.Parse()

	if *psSubscription == "" {
		return nil, fmt.Errorf("missing pubsub subscription config flag")
	}
	if *psEventTopic == "" {
		return nil, fmt.Errorf("missing pubsub event config flag")
	}
	if *workingDir == "" {
		return nil, fmt.Errorf("missing workdir config flag")
	}
	if *storageURI == "" {
		return nil, fmt.Errorf("missing storage-uri config flag")
	}
	gsProviderBucketsMap := []string{}
	if *gsProviderBuckets != "" {
		gsProviderBucketsMap = strings.Split(*gsProviderBuckets, ",")
	}
	return &config{
		Project:           *project,
		PsSubscription:    *psSubscription,
		PsEventTopic:      *psEventTopic,
		StorageURI:        *storageURI,
		WorkingDir:        *workingDir,
		LocalProviderPath: *localProviderPath,
		PepsUsername:      *pepsUsername,
		PepsPassword:      *pepsPassword,
		OndaUsername:      *ondaUsername,
		OndaPassword:      *ondaPassword,
		ScihubUsername:    *scihubUsername,
		ScihubPassword:    *scihubPassword,
		CreodiasUsername:  *creodiasUsername,
		CreodiasPassword:  *creodiasPassword,
		SoblooApiKey:      *soblooApiKey,
		MundiSeeedToken:   *mundiSeeedToken,
		GSProviderBuckets: gsProviderBucketsMap,
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

	var jobConsumer messaging.Consumer
	var eventPublisher messaging.Publisher
	var logMessaging string
	{
		if config.PsSubscription != "" {
			logMessaging += fmt.Sprintf(" pulling on %s/%s", config.Project, config.PsSubscription)
			if jobConsumer, err = pubsub.NewConsumer(config.Project, config.PsSubscription); err != nil {
				return fmt.Errorf("pubsub.NewConsumer: %w", err)
			}
		}
		if config.PsEventTopic != "" {
			logMessaging += fmt.Sprintf(" pulling on %s/%s", config.Project, config.PsEventTopic)
			eventTopic, err := pubsub.NewPublisher(ctx, config.Project, config.PsEventTopic, pubsub.WithMaxRetries(5))
			if err != nil {
				return fmt.Errorf("pubsub.NewPublisher: %w", err)
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

	maxTries := 15 //Must be less than the configured number of tries of the pubsub topic
	log.Logger(ctx).Debug("downloader starts " + logMessaging + " downloading image from " + strings.Join(providerNames, ", "))
	for {
		err := jobConsumer.Pull(ctx, func(ctx context.Context, msg *messaging.Message) (finalerr error) {
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

			defer func() {
				if finalerr != nil {
					return
				}
				res := common.Result{
					Type:    common.ResultTypeScene,
					ID:      scene.ID,
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

			if err := downloader.ProcessScene(ctx, imageProviders, storageService, scene, config.WorkingDir); err != nil {
				if msg.TryCount >= maxTries {
					log.Logger(ctx).Error("failing job after too many retries", zap.Error(err))
					message = err.Error()
				} else if !service.Temporary(err) {
					log.Logger(ctx).Error("job failed", zap.Error(err))
					message = err.Error()
				} else {
					log.Logger(ctx).Warn("job temporary failure", zap.Error(err))
					finalerr = err
				}
				return
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
