package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog"
	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/interface/catalog/oneatlas"
	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type config struct {
	Area   string
	Scenes string

	GeocubeServer         string
	GeocubeServerInsecure bool
	GeocubeServerApiKey   string
	AnnotationsURLs       []string
	WorkflowServer        string
	WorkflowToken         string
	ProcessingDir         string
	OneAtlasUsername      string
	OneAtlasApikey        string
	OneAtlasEndpoint      string
	CopernicusCatalog     bool
	CreodiasCatalog       bool
	LandsatAwsCatalog     bool
}

func newAppConfig() (*config, error) {
	var annotationsURLs string
	config := config{}
	flag.StringVar(&config.Area, "area", "", "Json of the area to process")
	flag.StringVar(&config.Scenes, "scenes", "", "Json of the scenes to send to the workflow server (shortcut to reuse intermediate results)")

	flag.StringVar(&config.GeocubeServer, "geocube-server", "", "address of geocube server")
	flag.BoolVar(&config.GeocubeServerInsecure, "geocube-insecure", false, "connection to geocube server is insecure (if geocube-server is started without -tls option)")
	flag.StringVar(&config.GeocubeServerApiKey, "geocube-apikey", "", "geocube server api key")
	flag.StringVar(&annotationsURLs, "annotations-urls", "", "URL (local/gs/aws) containing S1-scenes (as zip) to read annotations without downloading the whole file (optional, contains identifiers between brackets that will be replaced by those of the scene. E.g: gs://bucket/{DATE}/{SCENE}.zip), several urls are coma separated")
	flag.StringVar(&config.WorkflowServer, "workflow-server", "", "address of workflow server")
	flag.StringVar(&config.WorkflowToken, "workflow-token", "", "address of workflow server")
	flag.StringVar(&config.ProcessingDir, "workdir", "", "working directory to store intermediate results (could be empty or temporary)")
	flag.StringVar(&config.OneAtlasUsername, "oneatlas-username", "APIKEY", "oneatlas account username (optional). To configure Oneatlas as a potential image Provider.")
	flag.StringVar(&config.OneAtlasApikey, "oneatlas-apikey", "", "oneatlas account password (optional)")
	flag.StringVar(&config.OneAtlasEndpoint, "oneatlas-endpoint", oneatlas.OneAtlasSearchEndpoint, "oneatlas endpoint to search products from the catalogue")
	flag.BoolVar(&config.CopernicusCatalog, "copernicus-catalog", false, "Use the copernicus catalog service (search data)")
	flag.BoolVar(&config.CreodiasCatalog, "creodias-catalog", false, "Use the creodias catalog service (search data)")
	flag.BoolVar(&config.LandsatAwsCatalog, "landsat-aws-catalog", false, "Use the Landsat AWS catalog service (search data)")
	flag.Parse()

	if len(annotationsURLs) > 0 {
		config.AnnotationsURLs = strings.Split(annotationsURLs, ",")
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

var c catalog.Catalog

func run(ctx context.Context) error {
	config, err := newAppConfig()
	if err != nil {
		return err
	}

	c = catalog.Catalog{}
	{
		// Geocube client
		if config.GeocubeServer != "" {
			var tlsConfig *tls.Config
			if !config.GeocubeServerInsecure {
				tlsConfig = &tls.Config{}
			}
			if c.GeocubeClient, err = service.NewGeocubeClient(ctx, config.GeocubeServer, config.GeocubeServerApiKey, tlsConfig); err != nil {
				return err
			}
		}

		// Connection to the external catalogue service
		// GCS Storage
		c.AnnotationsURLs = config.AnnotationsURLs

		// Workflow Server
		if config.WorkflowServer != "" {
			c.Workflow = catalog.RemoteWorkflowManager{Server: config.WorkflowServer, Token: config.WorkflowToken}
		}

		// Working dir
		c.WorkingDir = config.ProcessingDir

		// OneAtlas connection
		c.OneAtlasCatalogUser = config.OneAtlasUsername
		c.OneAtlasApikey = config.OneAtlasApikey
		c.OneAtlasCatalogEndpoint = config.OneAtlasEndpoint

		// Copernicus catalogue
		c.CopernicusCatalog = config.CopernicusCatalog

		// Creodias catalogue
		c.CreodiasCatalog = config.CreodiasCatalog

		// Landsat AWS catalogue
		c.LandsatAwsCatalog = config.LandsatAwsCatalog
	}

	if config.Area != "" {
		if config.Scenes != "" {
			return sendScenes(ctx, config.Area, config.Scenes)
		}
		return ingestArea(ctx, config.Area)
	}

	// HTTP Server
	r := mux.NewRouter()
	c.AddHandler(r)
	s := http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Logger(ctx).Fatal("catalog.ListenAndServe", zap.Error(err))
		}
	}()

	<-ctx.Done()
	sctx, cncl := context.WithTimeout(context.Background(), 30*time.Second)
	defer cncl()
	return s.Shutdown(sctx)
}

func ingestArea(ctx context.Context, jsonPath string) error {
	area := entities.AreaToIngest{}
	jsonFile, err := os.Open(jsonPath)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	byteValue, _ := io.ReadAll(jsonFile)
	if err = json.Unmarshal(byteValue, &area); err != nil {
		return err
	}

	var workingDir string
	if c.WorkingDir != "" {
		workingDir = filepath.Join(c.WorkingDir, uuid.New().String())
	}

	if err = os.MkdirAll(workingDir, 0766); err != nil {
		return service.MakeTemporary(fmt.Errorf("make directory %s: %w", workingDir, err))
	}
	defer func() {
		if err == nil {
			os.RemoveAll(workingDir)
		} else {
			fmt.Print("Catalog failed. Temporary files are still available here : " + workingDir)
		}
	}()
	if err = os.Chdir(workingDir); err != nil {
		return service.MakeTemporary(fmt.Errorf("chdir: %w", err))
	}

	_, err = c.IngestArea(ctx, area, entities.Scenes{}, entities.Scenes{}, workingDir)
	return err
}

func sendScenes(ctx context.Context, areaJsonPath, scenesJsonPath string) error {
	area := entities.AreaToIngest{}
	jsonFile, err := os.Open(areaJsonPath)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	byteValue, _ := io.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &area)

	scenesToIngest := struct {
		Scenes []common.SceneToIngest
	}{}
	jsonFile2, err := os.Open(scenesJsonPath)
	if err != nil {
		return err
	}
	defer jsonFile2.Close()
	byteValue, _ = io.ReadAll(jsonFile2)
	json.Unmarshal(byteValue, &scenesToIngest)
	_, err = c.PostScenes(ctx, area, scenesToIngest.Scenes)
	return err
}

/*
func scenesFromJSON(workingdir, filename string) ([]*Scene, error) {
	scenes := struct {
		Scenes []*Scene
	}{}
	file, err := io.ReadFile(workingdir + "/" + filename)
	if err != nil {
		return nil, fmt.Errorf("scenesFromJSON.ReadFile: %w", err)
	}
	if err := json.Unmarshal(file, &scenes); err != nil {
		return nil, fmt.Errorf("scenesFromJSON.Unmarshal: %w", err)
	}
	return scenes.Scenes, nil
}
*/
