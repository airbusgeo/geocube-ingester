package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/airbusgeo/geocube-ingester/common"
	db "github.com/airbusgeo/geocube-ingester/interface/database"
	"github.com/airbusgeo/geocube-ingester/service"
)

// WorkflowManager is an interface to some functions of the Workflow component
type WorkflowManager interface {
	// RootTiles from the workflow server (tiles that have no previous tile)
	RootTiles(ctx context.Context, aoi string) ([]common.Tile, error)
	// LeafTiles from the workflow server (tiles that is not the previous of any tiles)
	LeafTiles(ctx context.Context, aoi string) ([]common.Tile, error)
	// Create an AOI in the workflow server
	CreateAOI(ctx context.Context, aoi string) error
	// IngestScenes adds new scenes to the workflow and starts the processing
	// returns id per sourceID of the scenes ingested. If a scene already exists, the sourceID is not in the returned map
	IngestScenes(ctx context.Context, aoi string, scene ...common.SceneToIngest) (map[string]int, error)
}

type RemoteWorkflowManager struct {
	Server, Token string
}

// RootTiles implements WorkflowManager
func (rwm RemoteWorkflowManager) RootTiles(ctx context.Context, aoiID string) ([]common.Tile, error) {
	return rwm.getTiles(ctx, aoiID, "roottiles")
}

// LeafTiles implements WorkflowManager
func (rwm RemoteWorkflowManager) LeafTiles(ctx context.Context, aoiID string) ([]common.Tile, error) {
	return rwm.getTiles(ctx, aoiID, "leaftiles")
}

// getTiles implements WorkflowManager
func (rwm RemoteWorkflowManager) getTiles(ctx context.Context, aoi, endpoint string) ([]common.Tile, error) {
	body, err := service.HTTPGetWithAuth(ctx, rwm.Server+"/aoi/"+aoi+"/"+endpoint, "", "", rwm.Token)
	if err != nil {
		return nil, fmt.Errorf(endpoint+".%w", err)
	}
	tiles := []common.Tile{}
	if err = json.Unmarshal(body, &tiles); err != nil {
		return nil, fmt.Errorf(endpoint+".Unmarshal: %w", err)
	}
	return tiles, nil
}

// CreateAOI implements WorkflowManager
func (rwm RemoteWorkflowManager) CreateAOI(ctx context.Context, aoi string) error {
	resp, err := service.HTTPPostWithAuth(ctx, rwm.Server+"/aoi/"+aoi, bytes.NewBuffer(nil), "", "", rwm.Token)
	if err != nil {
		return fmt.Errorf("CreateAOI: %w", err)
	}
	if resp.StatusCode != 200 && resp.StatusCode != 204 && resp.StatusCode != 409 {
		return fmt.Errorf("CreateAOI: %s", resp.Status)
	}
	return nil
}

// IngestScene implements WorkflowManager
func (rwm RemoteWorkflowManager) IngestScene(ctx context.Context, aoi string, scene common.SceneToIngest) (int, error) {
	sceneb, err := json.Marshal(scene)
	if err != nil {
		return -1, fmt.Errorf("IngestScene.Marshal: %w", err)
	}
	resp, err := service.HTTPPostWithAuth(ctx, rwm.Server+"/aoi/"+aoi+"/scene", bytes.NewBuffer(sceneb), "", "", rwm.Token)
	if err != nil {
		return -1, fmt.Errorf("IngestScene.%w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 409 {
		return -1, db.ErrAlreadyExists{}
	}
	bodyResponse, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1, fmt.Errorf("IngestScene.ReadAll(%s): %w", string(bodyResponse), err)
	}

	sceneJSON := struct {
		ID int `json:"id"`
	}{}
	if err := json.Unmarshal(bodyResponse, &sceneJSON); err != nil {
		return -1, fmt.Errorf("IngestScene.Unmarshal(%s): %w", string(bodyResponse), err)
	}

	return sceneJSON.ID, nil
}

func (rwm RemoteWorkflowManager) IngestScenes(ctx context.Context, aoi string, scenes ...common.SceneToIngest) (map[string]int, error) {
	ids := map[string]int{}
	for _, scene := range scenes {
		id, err := rwm.IngestScene(ctx, aoi, scene)
		if err != nil {
			if !errors.As(err, &db.ErrAlreadyExists{}) {
				return nil, err
			}
		} else {
			ids[scene.SourceID] = id
		}
	}
	return ids, nil
}
