package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/airbusgeo/geocube-ingester/catalog"
	"github.com/airbusgeo/geocube-ingester/catalog/entities"
	"github.com/airbusgeo/geocube-ingester/common"
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
	ScihubUsername        string
	ScihubPassword        string
	GCSAnnotationsBucket  string
	WorkflowServer        string
	ProcessingDir         string
}

func newAppConfig() (*config, error) {
	area := flag.String("area", "", "Json of the area to process")
	scenes := flag.String("scenes", "", "Json of the scenes to send to the workflow server (shortcut to reuse intermediate results)")

	geocubeServer := flag.String("geocube-server", "", "address of geocube server")
	geocubeServerInsecure := flag.Bool("geocube-insecure", false, "connection to geocube server is insecure")
	geocubeServerApiKey := flag.String("geocube-apikey", "", "geocube server api key")
	scihubUsername := flag.String("scihub-username", "", "username to connect to the scihub catalog service")
	scihubPassword := flag.String("scihub-password", "", "password to connect to the scihub catalog service")
	gcsAnnotations := flag.String("gcs-annotations-bucket", "", "GCS bucket where scenes are stored (for annotations) (optional)")
	workflowServer := flag.String("workflow-server", "", "address of workflow server")
	processingDir := flag.String("workdir", "", "working directory to store intermediate results (could be empty or temporary)")
	flag.Parse()

	if *geocubeServer == "" {
		return nil, fmt.Errorf("missing geocube server flag")
	}
	if *workflowServer == "" {
		return nil, fmt.Errorf("missing workflow server config flag")
	}
	return &config{
		Area:                  *area,
		Scenes:                *scenes,
		GeocubeServer:         *geocubeServer,
		GeocubeServerInsecure: *geocubeServerInsecure,
		GeocubeServerApiKey:   *geocubeServerApiKey,
		ScihubUsername:        *scihubUsername,
		ScihubPassword:        *scihubPassword,
		GCSAnnotationsBucket:  *gcsAnnotations,
		WorkflowServer:        *workflowServer,
		ProcessingDir:         *processingDir,
	}, nil
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
		var tlsConfig *tls.Config
		if !config.GeocubeServerInsecure {
			tlsConfig = &tls.Config{}
		}
		if c.GeocubeClient, err = service.NewGeocubeClient(ctx, config.GeocubeServer, config.GeocubeServerApiKey, tlsConfig); err != nil {
			return err
		}

		// Connection to the external catalogue service
		// Scihub connection
		c.ScihubUser = config.ScihubUsername
		c.ScihubPword = config.ScihubPassword
		// GCS Storage
		c.GCSAnnotationsBucket = config.GCSAnnotationsBucket

		// Workflow Server
		c.WorkflowServer = config.WorkflowServer

		// Working dir
		c.WorkingDir = config.ProcessingDir
	}

	if config.Area != "" {
		if config.Scenes != "" {
			return sendScenes(ctx, config.Area, config.Scenes)
		}
		return ingestArea(ctx, config.Area)
	}

	// HTTP Server
	r := mux.NewRouter()
	r.HandleFunc("/ingest", ingestAreaHandler).Methods("POST")
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
	byteValue, _ := ioutil.ReadAll(jsonFile)
	if err = json.Unmarshal(byteValue, &area); err != nil {
		return err
	}

	_, err = IngestArea(ctx, area)
	return err
}

func sendScenes(ctx context.Context, areaJsonPath, scenesJsonPath string) error {
	area := entities.AreaToIngest{}
	jsonFile, err := os.Open(areaJsonPath)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &area)

	scenesToIngest := struct {
		Scenes []common.SceneToIngest
	}{}
	jsonFile2, err := os.Open(scenesJsonPath)
	if err != nil {
		return err
	}
	defer jsonFile2.Close()
	byteValue, _ = ioutil.ReadAll(jsonFile2)
	json.Unmarshal(byteValue, &scenesToIngest)
	_, err = postScenesToIngest(ctx, c.WorkflowServer, area, scenesToIngest.Scenes)
	return err
}

func ingestAreaHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	area := entities.AreaToIngest{}
	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&area); err != nil {
		w.WriteHeader(400)
		return
	}

	result, err := IngestArea(ctx, area)
	if err != nil {
		log.Printf("ingest: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	if resultb, err := json.Marshal(result); err != nil {
		fmt.Fprint(w, err.Error())
	} else {
		fmt.Fprint(w, resultb)
	}
}

// IngestAreaResult is the output of the catalog
type IngestAreaResult struct {
	ScenesID map[string]int `json:"scenes_id"`
	TilesNb  int            `json:"tiles_nb"`
}

func IngestArea(ctx context.Context, area entities.AreaToIngest) (IngestAreaResult, error) {
	var (
		err            error
		scenes         []*entities.Scene
		rootLeafTiles  []common.Tile
		workingDir     string
		scenesToIngest []common.SceneToIngest
		result         IngestAreaResult
	)

	if err := c.ValidateArea(&area); err != nil {
		return result, fmt.Errorf("IngestArea.%w", err)
	}

	if c.WorkingDir != "" {
		workingDir = filepath.Join(c.WorkingDir, uuid.New().String())
	}

	if err = os.MkdirAll(workingDir, 0766); err != nil {
		return result, service.MakeTemporary(fmt.Errorf("make directory %s: %w", workingDir, err))
	}
	defer func() {
		if err == nil {
			os.RemoveAll(workingDir)
		} else {
			fmt.Print("Catalog failed. Temporary files are still available here : " + workingDir)
		}
	}()
	if err = os.Chdir(workingDir); err != nil {
		return result, service.MakeTemporary(fmt.Errorf("chdir: %w", err))
	}

	// Scene inventory
	if scenes, err = c.DoScenesInventory(ctx, area); err != nil {
		return result, fmt.Errorf("ingestArea.%w", err)
	}
	toJSON(struct{ Scenes []*entities.Scene }{Scenes: scenes}, workingDir, "scenesInventory.json")

	switch entities.GetConstellation(area.SceneType.Constellation) {
	case entities.Sentinel1:
		// Get RootTiles
		if rootLeafTiles, err = getRootTiles(c.WorkflowServer, area.AOIID); err != nil {
			return result, fmt.Errorf("ingestArea.%w", err)
		}
		// Get LeafTiles
		leafTiles, err := getLeafTiles(c.WorkflowServer, area.AOIID)
		if err != nil {
			return result, fmt.Errorf("ingestArea.%w", err)
		}
		rootLeafTiles = append(rootLeafTiles, leafTiles...)
	}

	// Tiles inventory
	result.TilesNb, err = c.DoTilesInventory(ctx, area, scenes, rootLeafTiles)
	if err != nil {
		return result, fmt.Errorf("ingestArea.%w", err)
	}
	toJSON(struct{ Scenes []*entities.Scene }{Scenes: scenes}, workingDir, "tilesInventory.json")

	defer func() {
		c.DeletePendingRecords(ctx, scenes, result.ScenesID)
	}()

	// Create scenes to ingest
	log.Logger(ctx).Debug("Create scenes to ingest")
	scenesToIngest, err = c.ScenesToIngest(ctx, area, scenes)
	if err != nil {
		return result, fmt.Errorf("ingestArea.%w", err)
	}
	toJSON(struct{ Scenes []common.SceneToIngest }{Scenes: scenesToIngest}, workingDir, "scenesToIngest.json")

	// Post scenes
	log.Logger(ctx).Debug("Post scenes to ingest")
	if result.ScenesID, err = postScenesToIngest(ctx, c.WorkflowServer, area, scenesToIngest); err != nil {
		return result, fmt.Errorf("ingestArea.%w", err)
	}
	log.Logger(ctx).Debug("Done !")

	return result, err
}

func toJSON(v interface{}, workingdir, filename string) error {
	if workingdir != "" {
		vb, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("toJSON.Marshal: %w", err)
		}
		if err := ioutil.WriteFile(workingdir+"/"+filename, vb, 0644); err != nil {
			return fmt.Errorf("toJSON.WriteFile: %w", err)
		}
	}
	return nil
}

/*
func scenesFromJSON(workingdir, filename string) ([]*Scene, error) {
	scenes := struct {
		Scenes []*Scene
	}{}
	file, err := ioutil.ReadFile(workingdir + "/" + filename)
	if err != nil {
		return nil, fmt.Errorf("scenesFromJSON.ReadFile: %w", err)
	}
	if err := json.Unmarshal(file, &scenes); err != nil {
		return nil, fmt.Errorf("scenesFromJSON.Unmarshal: %w", err)
	}
	return scenes.Scenes, nil
}
*/

// getRootTiles form the workflow server
func getRootTiles(workflowServer, aoiID string) ([]common.Tile, error) {
	resp, err := http.Get(workflowServer + "/aoi/" + aoiID + "/roottiles")
	if err != nil {
		return nil, fmt.Errorf("getRootTiles.Get: %w", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("getRootTiles.ReadAll: %w", err)
	}
	tiles := []common.Tile{}
	if err = json.Unmarshal(body, &tiles); err != nil {
		return nil, fmt.Errorf("getRootTiles.Unmarshal: %w", err)
	}
	return tiles, nil
}

// getLeafTiles form the workflow server
func getLeafTiles(workflowServer, aoiID string) ([]common.Tile, error) {
	resp, err := http.Get(workflowServer + "/aoi/" + aoiID + "/leaftiles")
	if err != nil {
		return nil, fmt.Errorf("getLeafTiles.Get: %w", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("getLeafTiles.ReadAll: %w", err)
	}
	tiles := []common.Tile{}
	if err = json.Unmarshal(body, &tiles); err != nil {
		return nil, fmt.Errorf("getLeafTiles.Unmarshal: %w", err)
	}
	return tiles, nil
}

// postScenesToIngest sends the sceneToIngest to the workflow server
// Returns the id of the posted scenes (even if PostScenesToIngest returns an error)
func postScenesToIngest(ctx context.Context, workflowServer string, area entities.AreaToIngest, scenesToIngest []common.SceneToIngest) (map[string]int, error) {
	ids := map[string]int{}

	// First, create AOI
	resp, err := http.Post(workflowServer+"/aoi/"+area.AOIID, "application/json", bytes.NewBuffer(nil))
	if err != nil {
		return ids, fmt.Errorf("PostScenesToIngest.PostAOI: %w", err)
	}
	if resp.StatusCode != 200 && resp.StatusCode != 204 && resp.StatusCode != 409 {
		return ids, fmt.Errorf("PostScenesToIngest.PostAOI: %s", resp.Status)
	}

	// Then, send scenes
	for _, scene := range scenesToIngest {
		sceneb, err := json.Marshal(scene)
		if err != nil {
			return ids, fmt.Errorf("PostScenesToIngest.Marshal: %w", err)
		}
		resp, err := http.Post(c.WorkflowServer+"/aoi/"+area.AOIID+"/scene", "application/json", bytes.NewBuffer(sceneb))
		if err != nil {
			return ids, fmt.Errorf("PostScenesToIngest.PostScenes: %w", err)
		}
		if resp.StatusCode == 409 {
			//Scene already exists
			log.Logger(ctx).Sugar().Warnf("Scene %s already exists", scene.SourceID)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return ids, fmt.Errorf("PostScenesToIngest.ReadAll: %w", err)
		}
		if resp.StatusCode != 200 {
			return ids, fmt.Errorf("PostScenesToIngest.PostScenes: %s", string(body))
		}
		sceneID := struct {
			ID int `json:"id"`
		}{}
		if err := json.Unmarshal(body, &sceneID); err != nil {
			return ids, fmt.Errorf("PostScenesToIngest.Unmarshal(%s): %w", string(body), err)
		}
		ids[scene.SourceID] = sceneID.ID
	}
	return ids, nil
}
