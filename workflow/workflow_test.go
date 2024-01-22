package workflow_test

import (
	"encoding/json"
	"time"

	"github.com/airbusgeo/geocube-ingester/common"
	db "github.com/airbusgeo/geocube-ingester/interface/database"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Workflow", func() {
	var err error
	aoi := "test"
	rootSceneToIngest := common.SceneToIngest{
		Scene: common.Scene{
			SourceID: "S1B_IW_SLC__1SDV_20180806T170022_20180806T170050_012145_0165D7_27D6",
			AOI:      aoi,
			Data: common.SceneAttrs{
				UUID: "05a23a04-82fa-46e0-b9a9-2c25912a305c",
				Date: time.Date(2019, 8, 25, 17, 1, 13, 0, time.Local),
				TileMappings: map[string]common.TileMapping{
					"A44_IW1_8951": {SwathID: "IW1", TileNr: 4},
					"A44_IW1_8979": {SwathID: "IW1", TileNr: 5},
				},
				GraphName:   "S1Preprocessing",
				GraphConfig: map[string]string{"projection": "EPSG:32632"},
				RecordID:    "05a3b01d-4b30-4573-94d6-8d83f1dbb2ff",
				InstancesID: map[string]string{"coh_VH": "4c8acc94-7b23-497b-8d31-8845a9ea76d2"},
			},
		},
		Tiles: map[string]common.TileToIngest{
			"A44_IW1_8951": {
				PreviousTileID:   "",
				PreviousSceneID:  "",
				ReferenceTileID:  "",
				ReferenceSceneID: "",
				Data:             common.TileAttrs{SwathID: "IW1", TileNr: 4, GraphName: "S1BackscatterCoherence"},
			},
			"A44_IW1_8979": {
				PreviousTileID:   "",
				PreviousSceneID:  "",
				ReferenceTileID:  "",
				ReferenceSceneID: "",
				Data:             common.TileAttrs{SwathID: "IW1", TileNr: 5, GraphName: "S1BackscatterCoherence"},
			},
		},
		RetryCount: 2,
	}
	leaf1SceneToIngest := common.SceneToIngest{
		Scene: common.Scene{
			SourceID: "S1B_IW_SLC__1SDV_20190825T170029_20190825T170057_017745_02163A_DA7E",
			AOI:      aoi,
			Data: common.SceneAttrs{
				UUID: "05a23a04-82fa-46e0-b9a9-2c25912a305c",
				Date: time.Date(2019, 8, 31, 17, 1, 13, 0, time.Local),
				TileMappings: map[string]common.TileMapping{
					"A44_IW1_8951": {SwathID: "IW1", TileNr: 4},
					"A44_IW1_8979": {SwathID: "IW1", TileNr: 5},
				},
				GraphName:   "S1Preprocessing",
				GraphConfig: map[string]string{"projection": "EPSG:32632"},
				RecordID:    "05a3b01d-4b30-4573-94d6-8d83f1dbb2ff",
				InstancesID: map[string]string{"coh_VH": "4c8acc94-7b23-497b-8d31-8845a9ea76d2"},
			},
		},
		Tiles: map[string]common.TileToIngest{
			"A44_IW1_8951": {
				PreviousTileID:   "A44_IW1_8951",
				PreviousSceneID:  "S1B_IW_SLC__1SDV_20180806T170022_20180806T170050_012145_0165D7_27D6",
				ReferenceTileID:  "A44_IW1_8951",
				ReferenceSceneID: "S1B_IW_SLC__1SDV_20180806T170022_20180806T170050_012145_0165D7_27D6",
				Data:             common.TileAttrs{SwathID: "IW1", TileNr: 4, GraphName: "S1BackscatterCoherence"},
			},
			"A44_IW1_8979": {
				PreviousTileID:   "A44_IW1_8979",
				PreviousSceneID:  "S1B_IW_SLC__1SDV_20180806T170022_20180806T170050_012145_0165D7_27D6",
				ReferenceTileID:  "A44_IW1_8979",
				ReferenceSceneID: "S1B_IW_SLC__1SDV_20180806T170022_20180806T170050_012145_0165D7_27D6",
				Data:             common.TileAttrs{SwathID: "IW1", TileNr: 5, GraphName: "S1BackscatterCoherence"},
			},
		},
	}
	leaf2SceneToIngest := common.SceneToIngest{
		Scene: common.Scene{
			SourceID: "S1A_IW_SLC__1SDV_20190831T170113_20190831T170140_028816_0343BD_C4B2",
			AOI:      aoi,
			Data: common.SceneAttrs{
				UUID: "05a23a04-82fa-46e0-b9a9-2c25912a305c",
				Date: time.Date(2019, 9, 5, 17, 1, 13, 0, time.Local),
				TileMappings: map[string]common.TileMapping{
					"A44_IW1_8951": {SwathID: "IW1", TileNr: 4},
					"A44_IW1_8979": {SwathID: "IW1", TileNr: 5},
				},
				GraphName:   "S1Preprocessing",
				GraphConfig: map[string]string{"projection": "EPSG:32632"},
				RecordID:    "05a3b01d-4b30-4573-94d6-8d83f1dbb2ff",
				InstancesID: map[string]string{"coh_VH": "4c8acc94-7b23-497b-8d31-8845a9ea76d2"},
			},
		},
		Tiles: map[string]common.TileToIngest{
			"A44_IW1_8951": {
				PreviousTileID:   "A44_IW1_8951",
				PreviousSceneID:  "S1B_IW_SLC__1SDV_20190825T170029_20190825T170057_017745_02163A_DA7E",
				ReferenceTileID:  "A44_IW1_8951",
				ReferenceSceneID: "S1B_IW_SLC__1SDV_20180806T170022_20180806T170050_012145_0165D7_27D6",
				Data:             common.TileAttrs{SwathID: "IW1", TileNr: 4, GraphName: "S1BackscatterCoherence"},
			},
			"A44_IW1_8979": {
				PreviousTileID:   "A44_IW1_8979",
				PreviousSceneID:  "S1B_IW_SLC__1SDV_20190825T170029_20190825T170057_017745_02163A_DA7E",
				ReferenceTileID:  "A44_IW1_8979",
				ReferenceSceneID: "S1B_IW_SLC__1SDV_20180806T170022_20180806T170050_012145_0165D7_27D6",
				Data:             common.TileAttrs{SwathID: "IW1", TileNr: 5, GraphName: "S1BackscatterCoherence"},
			},
		},
	}

	initDbScenesTiles := func(scenesDone bool) (int, int, int, int, int, int) {
		_, err = pgdb.ExecContext(ctx, "DELETE from public.tile")
		Expect(err).NotTo(HaveOccurred())
		_, err = pgdb.ExecContext(ctx, "DELETE from public.scene")
		Expect(err).NotTo(HaveOccurred())
		_, err := pgdb.ExecContext(ctx, "DELETE from public.aoi")
		Expect(err).NotTo(HaveOccurred())
		err = wf.CreateAOI(ctx, aoi)
		Expect(err).NotTo(HaveOccurred())
		ids0, err := wf.IngestScene(ctx, aoi, rootSceneToIngest)
		Expect(err).NotTo(HaveOccurred())
		ids1, err := wf.IngestScene(ctx, aoi, leaf1SceneToIngest)
		Expect(err).NotTo(HaveOccurred())
		ids2, err := wf.IngestScene(ctx, aoi, leaf2SceneToIngest)
		Expect(err).NotTo(HaveOccurred())
		tiles, err := wf.Tiles(ctx, "", ids2, "", false, 0, -1)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(tiles)).To(Equal(2))
		Expect(tiles[0].PreviousID).NotTo(Equal(nil))
		idb2, idb1 := tiles[0].ID, *tiles[0].PreviousID
		tile, _, err := wf.Tile(ctx, idb1, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(tile.PreviousID).NotTo(Equal(nil))
		idb0 := *tile.PreviousID
		if scenesDone {
			wf.ResultHandler(ctx, common.Result{
				Type:   common.ResultTypeScene,
				ID:     ids0,
				Status: common.StatusDONE,
			})
			wf.ResultHandler(ctx, common.Result{
				Type:   common.ResultTypeScene,
				ID:     ids1,
				Status: common.StatusDONE,
			})
			wf.ResultHandler(ctx, common.Result{
				Type:   common.ResultTypeScene,
				ID:     ids2,
				Status: common.StatusDONE,
			})
		}

		return ids0, ids1, ids2, idb0, idb1, idb2
	}

	expectTileToProcessToBeIngested := func(tile common.TileToProcess, scene common.SceneToIngest) {
		tileToIngest, ok := scene.Tiles[tile.SourceID]
		Expect(ok).To(BeTrue())
		Expect(tile.Scene.SourceID).To(Equal(scene.SourceID))
		Expect(tile.Previous.SourceID).To(Equal(tileToIngest.PreviousTileID))
		Expect(tile.Previous.Scene.SourceID).To(Equal(tileToIngest.PreviousSceneID))
		Expect(tile.Reference.SourceID).To(Equal(tileToIngest.ReferenceTileID))
		Expect(tile.Reference.Scene.SourceID).To(Equal(tileToIngest.ReferenceSceneID))
	}

	expectTileToBeIngested := func(tile common.Tile, scene common.SceneToIngest) {
		tileToIngest, ok := scene.Tiles[tile.SourceID]
		Expect(ok).To(BeTrue())
		Expect(tile.Scene.SourceID).To(Equal(scene.SourceID))
		Expect(tile.Data).To(Equal(tileToIngest.Data))
	}

	BeforeEach(func() {
		tileQueue.messages = nil
		sceneQueue.messages = nil
	})

	Describe("Creating AOI", func() {
		BeforeEach(func() {
			_, err := pgdb.ExecContext(ctx, "DELETE from public.aoi")
			Expect(err).NotTo(HaveOccurred())
		})

		Context("With empty aoi table", func() {
			JustBeforeEach(func() {
				err := wf.CreateAOI(ctx, aoi)
				Expect(err).NotTo(HaveOccurred())
			})
			It("should create an aoi", func() {
				count := 0
				err := pgdb.QueryRowContext(ctx, "select count(*) from public.aoi where id='"+aoi+"'").Scan(&count)
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(1))
			})
		})

		Context("With an aoi table that already contains this aoi", func() {
			JustBeforeEach(func() {
				err = wf.CreateAOI(ctx, aoi)
				Expect(err).NotTo(HaveOccurred())
				err = wf.CreateAOI(ctx, aoi)
			})
			It("should return an AlreadyExists error", func() {
				Expect(err).To(Equal(db.ErrAlreadyExists{Type: "aoi", ID: aoi}))
			})
		})
	})

	Describe("Creating a Scene", func() {
		var sceneQueueLenBefore int
		BeforeEach(func() {
			_, err = pgdb.ExecContext(ctx, "DELETE from public.tile")
			Expect(err).NotTo(HaveOccurred())
			_, err = pgdb.ExecContext(ctx, "DELETE from public.scene")
			Expect(err).NotTo(HaveOccurred())
			_, err := pgdb.ExecContext(ctx, "DELETE from public.aoi")
			Expect(err).NotTo(HaveOccurred())
			err = wf.CreateAOI(ctx, aoi)
			Expect(err).NotTo(HaveOccurred())
		})

		Describe("That has no previous scenes", func() {
			Context("With empty scene table", func() {
				id := 0
				JustBeforeEach(func() {
					sceneQueueLenBefore = len(sceneQueue.messages)
					id, err = wf.IngestScene(ctx, aoi, rootSceneToIngest)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should create a scene with this name", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where aoi_id=$1 and source_id=$2", aoi, rootSceneToIngest.SourceID).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(1))
				})
				It("should create a scene with the returned id", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where id=$1", id).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(1))
				})
				It("should create a scene with the returned id", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where id=$1", id).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(1))
				})
				It("should create tiles", func() {
					tiles, err := wf.Tiles(ctx, "", id, "", false, 0, -1)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(tiles)).To(Equal(len(rootSceneToIngest.Tiles)))
					for _, t := range tiles {
						tile, ok := rootSceneToIngest.Tiles[t.SourceID]
						Expect(ok).To(Equal(true))
						Expect(t.RetryCountDown).To(Equal(rootSceneToIngest.RetryCount))
						Expect(tile.Data).To(Equal(t.Data))
					}
				})
				It("should post a message in sceneQueue", func() {
					Expect(len(sceneQueue.messages)).To(Equal(sceneQueueLenBefore + 1))
					scene := common.Scene{}
					Expect(json.Unmarshal(sceneQueue.messages[0], &scene)).NotTo(HaveOccurred())
					sceneToEqual := rootSceneToIngest.Scene
					sceneToEqual.ID = scene.ID
					Expect(scene).To(Equal(sceneToEqual))
					Expect(scene.ID).To(Equal(id))
				})
			})
		})

		Describe("That has one previous scene", func() {
			Context("With empty scene table", func() {
				JustBeforeEach(func() {
					_, err = wf.IngestScene(ctx, aoi, leaf1SceneToIngest)
					Expect(err).To(HaveOccurred())
				})
				It("should not create a scene with this name", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where aoi_id=$1 and source_id=$2", aoi, rootSceneToIngest.SourceID).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(0))
				})
			})

			Context("With a root scene", func() {
				id := 0
				JustBeforeEach(func() {
					sceneQueueLenBefore = len(sceneQueue.messages)
					id, err = wf.IngestScene(ctx, aoi, rootSceneToIngest)
					Expect(err).NotTo(HaveOccurred())
					id, err = wf.IngestScene(ctx, aoi, leaf1SceneToIngest)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should create a scene with this name", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where aoi_id=$1 and source_id=$2", aoi, leaf1SceneToIngest.SourceID).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(1))
				})
				It("should create a scene with the returned id", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where id=$1", id).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(1))
				})
				It("should post two messages in sceneQueue", func() {
					Expect(len(sceneQueue.messages)).To(Equal(sceneQueueLenBefore + 2))
					scene := common.Scene{}
					Expect(json.Unmarshal(sceneQueue.messages[1], &scene)).NotTo(HaveOccurred())
					sceneToEqual := leaf1SceneToIngest.Scene
					sceneToEqual.ID = scene.ID
					Expect(scene).To(Equal(sceneToEqual))
					Expect(scene.ID).To(Equal(id))
				})
			})
		})

		Describe("That has two previous scenes", func() {
			Context("With no previous scene", func() {
				JustBeforeEach(func() {
					_, err = wf.IngestScene(ctx, aoi, rootSceneToIngest)
					Expect(err).NotTo(HaveOccurred())
					_, err = wf.IngestScene(ctx, aoi, leaf2SceneToIngest)
					Expect(err).To(HaveOccurred())
				})
				It("should not create a scene with this name", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where aoi_id=$1 and source_id=$2", aoi, leaf2SceneToIngest.SourceID).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(0))
				})
			})
			Context("With previous and ref scene", func() {
				id := 0
				JustBeforeEach(func() {
					sceneQueueLenBefore = len(sceneQueue.messages)
					id, err = wf.IngestScene(ctx, aoi, rootSceneToIngest)
					Expect(err).NotTo(HaveOccurred())
					id, err = wf.IngestScene(ctx, aoi, leaf1SceneToIngest)
					Expect(err).NotTo(HaveOccurred())
					id, err = wf.IngestScene(ctx, aoi, leaf2SceneToIngest)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should create a scene with this name", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where aoi_id=$1 and source_id=$2", aoi, leaf2SceneToIngest.SourceID).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(1))
				})
				It("should create a scene with the returned id", func() {
					count := 0
					err := pgdb.QueryRowContext(ctx, "select count(*) from public.scene where id=$1", id).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(1))
				})
				It("should post three messages in sceneQueue", func() {
					Expect(len(sceneQueue.messages)).To(Equal(sceneQueueLenBefore + 3))
					scene := common.Scene{}
					Expect(json.Unmarshal(sceneQueue.messages[2], &scene)).NotTo(HaveOccurred())
					sceneToEqual := leaf2SceneToIngest.Scene
					sceneToEqual.ID = scene.ID
					Expect(scene).To(Equal(sceneToEqual))
					Expect(scene.ID).To(Equal(id))
				})
			})
		})
	})

	Describe("Finishing a scene", func() {
		var id0, id1 int
		var sceneQueueLenBefore, tileQueueLenBefore int
		BeforeEach(func() {
			tileQueue.messages = nil
			sceneQueue.messages = nil
			_, err = pgdb.ExecContext(ctx, "DELETE from public.tile")
			Expect(err).NotTo(HaveOccurred())
			_, err = pgdb.ExecContext(ctx, "DELETE from public.scene")
			Expect(err).NotTo(HaveOccurred())
			_, err := pgdb.ExecContext(ctx, "DELETE from public.aoi")
			Expect(err).NotTo(HaveOccurred())
			err = wf.CreateAOI(ctx, aoi)
			Expect(err).NotTo(HaveOccurred())
			id0, err = wf.IngestScene(ctx, aoi, rootSceneToIngest)
			Expect(err).NotTo(HaveOccurred())
			id1, err = wf.IngestScene(ctx, aoi, leaf1SceneToIngest)
			Expect(err).NotTo(HaveOccurred())
		})

		Describe("Which is a root scene", func() {
			Context("With success", func() {
				JustBeforeEach(func() {
					sceneQueueLenBefore = len(sceneQueue.messages)
					tileQueueLenBefore = len(tileQueue.messages)
					wf.ResultHandler(ctx, common.Result{
						Type:   common.ResultTypeScene,
						ID:     id0,
						Status: common.StatusDONE,
					})
				})
				It("should update the status of the scene", func() {
					scene, err := wf.Scene(ctx, id0, nil)
					Expect(err).NotTo(HaveOccurred())
					Expect(scene.Status).To(Equal(common.StatusDONE))
				})
				It("should update the status of its tiles", func() {
					tiles, err := wf.Tiles(ctx, "", id0, "", false, 0, -1)
					Expect(err).NotTo(HaveOccurred())
					for _, t := range tiles {
						Expect(t.Status).To(Equal(common.StatusPENDING))
					}
				})
				It("should post messages in tileQueue", func() {
					Expect(len(tileQueue.messages)).To(Equal(len(rootSceneToIngest.Tiles)))
					for _, m := range tileQueue.messages {
						tile := common.TileToProcess{}
						Expect(json.Unmarshal(m, &tile)).NotTo(HaveOccurred())
						expectTileToProcessToBeIngested(tile, rootSceneToIngest)
					}
				})
			})
			Context("With retry", func() {
				JustBeforeEach(func() {
					wf.ResultHandler(ctx, common.Result{
						Type:    common.ResultTypeScene,
						ID:      id0,
						Status:  common.StatusRETRY,
						Message: "error",
					})
				})
				It("should update the status of the scene", func() {
					scene, err := wf.Scene(ctx, id0, nil)
					Expect(err).NotTo(HaveOccurred())
					Expect(scene.Status).To(Equal(common.StatusPENDING))
					Expect(scene.RetryCountDown).To(Equal(rootSceneToIngest.RetryCount - 1))
					Expect(scene.Message).To(Equal(""))
				})
				It("should not update the status of its tiles", func() {
					tiles, err := wf.Tiles(ctx, "", id0, "", false, 0, -1)
					Expect(err).NotTo(HaveOccurred())
					for _, t := range tiles {
						Expect(t.Status).To(Equal(common.StatusNEW))
					}
				})
				It("should not post messages in tileQueue", func() {
					Expect(len(tileQueue.messages)).To(Equal(tileQueueLenBefore))
				})
				It("should post a message in sceneQueue", func() {
					Expect(len(sceneQueue.messages)).To(Equal(sceneQueueLenBefore + 1))
				})
			})
		})

		Describe("Which is not a root scene", func() {
			Context("And the previous scene is not finished", func() {
				JustBeforeEach(func() {
					wf.ResultHandler(ctx, common.Result{
						Type:   common.ResultTypeScene,
						ID:     id1,
						Status: common.StatusDONE,
					})
				})
				It("should not update the status of its tiles", func() {
					tiles, err := wf.Tiles(ctx, "", id1, "", false, 0, -1)
					Expect(err).NotTo(HaveOccurred())
					for _, t := range tiles {
						Expect(t.Status).To(Equal(common.StatusNEW))
					}
				})
			})
			Context("And one tile of the previous scene is finished", func() {
				JustBeforeEach(func() {
					tileQueueLenBefore = len(tileQueue.messages)
					wf.ResultHandler(ctx, common.Result{
						Type:   common.ResultTypeScene,
						ID:     id0,
						Status: common.StatusDONE,
					})
					tiles, err := wf.Tiles(ctx, "", id0, "", false, 0, -1)
					Expect(err).NotTo(HaveOccurred())
					wf.ResultHandler(ctx, common.Result{
						Type:   common.ResultTypeTile,
						ID:     tiles[0].ID,
						Status: common.StatusDONE,
					})
					tileQueue.messages = nil
					wf.ResultHandler(ctx, common.Result{
						Type:   common.ResultTypeScene,
						ID:     id1,
						Status: common.StatusDONE,
					})
				})
				It("should update the status of its tiles", func() {
					tiles, err := wf.Tiles(ctx, "", id1, "", false, 0, -1)
					Expect(err).NotTo(HaveOccurred())
					nbNew := 0
					nbPending := 0
					for _, t := range tiles {
						switch t.Status {
						case common.StatusNEW:
							nbNew++
						case common.StatusPENDING:
							nbPending++
						}
					}
					Expect(nbNew).To(Equal(1))
					Expect(nbPending).To(Equal(1))
				})
				It("should post one message in tileQueue", func() {
					Expect(len(tileQueue.messages)).To(Equal(tileQueueLenBefore + 1))
					tile := common.TileToProcess{}
					Expect(json.Unmarshal(tileQueue.messages[0], &tile)).NotTo(HaveOccurred())
					expectTileToProcessToBeIngested(tile, leaf1SceneToIngest)
				})
			})

			Context("With retry", func() {
				JustBeforeEach(func() {
					sceneQueueLenBefore = len(sceneQueue.messages)
					tileQueueLenBefore = len(tileQueue.messages)
					wf.ResultHandler(ctx, common.Result{
						Type:    common.ResultTypeScene,
						ID:      id1,
						Status:  common.StatusRETRY,
						Message: "error",
					})
				})
				It("should update the status of the scene", func() {
					scene, err := wf.Scene(ctx, id1, nil)
					Expect(err).NotTo(HaveOccurred())
					Expect(scene.Status).To(Equal(common.StatusRETRY))
					Expect(scene.RetryCountDown).To(Equal(0))
					Expect(scene.Message).To(Equal("error"))
				})
				It("should not update the status of its tiles", func() {
					tiles, err := wf.Tiles(ctx, "", id1, "", false, 0, -1)
					Expect(err).NotTo(HaveOccurred())
					for _, t := range tiles {
						Expect(t.Status).To(Equal(common.StatusNEW))
					}
				})
				It("should not post messages in tileQueue", func() {
					Expect(len(tileQueue.messages)).To(Equal(tileQueueLenBefore))
				})
				It("should not post a message in sceneQueue", func() {
					Expect(len(sceneQueue.messages)).To(Equal(sceneQueueLenBefore))
				})
			})
		})
	})

	Describe("Finishing a tile", func() {
		var idb0, idb1, idb2 int
		var tileQueueLenBefore int
		BeforeEach(func() {
			_, _, _, idb0, idb1, idb2 = initDbScenesTiles(true)
			tileQueue.messages = nil
			sceneQueue.messages = nil
		})

		Describe("Which is a root tile", func() {
			Context("With failure", func() {
				JustBeforeEach(func() {
					tileQueueLenBefore = len(tileQueue.messages)
					wf.ResultHandler(ctx, common.Result{
						Type:    common.ResultTypeTile,
						ID:      idb0,
						Status:  common.StatusRETRY,
						Message: "error",
					})
				})
				It("should update the status of the tile", func() {
					tile, _, err := wf.Tile(ctx, idb0, false)
					Expect(err).NotTo(HaveOccurred())
					Expect(tile.Status).To(Equal(common.StatusPENDING))
					Expect(tile.RetryCountDown).To(Equal(rootSceneToIngest.RetryCount - 1))
					Expect(tile.Message).To(Equal(""))
				})
				It("should post a retry message in tileQueue", func() {
					Expect(len(tileQueue.messages)).To(Equal(tileQueueLenBefore + 1))
				})
			})
			Context("With success", func() {
				JustBeforeEach(func() {
					tileQueueLenBefore = len(tileQueue.messages)
					wf.ResultHandler(ctx, common.Result{
						Type:   common.ResultTypeTile,
						ID:     idb0,
						Status: common.StatusDONE,
					})
				})
				It("should update the status of the tile", func() {
					tile, _, err := wf.Tile(ctx, idb0, false)
					Expect(err).NotTo(HaveOccurred())
					Expect(tile.Status).To(Equal(common.StatusDONE))
				})
				It("should update the status of the next tile", func() {
					tile, _, err := wf.Tile(ctx, idb1, false)
					Expect(err).NotTo(HaveOccurred())
					Expect(tile.Status).To(Equal(common.StatusPENDING))
					Expect(tile.RetryCountDown).To(Equal(0))
				})
				It("should not update the status of the final tile", func() {
					tile, _, err := wf.Tile(ctx, idb2, false)
					Expect(err).NotTo(HaveOccurred())
					Expect(tile.Status).To(Equal(common.StatusNEW))
				})
				It("should post one message in tileQueue", func() {
					Expect(len(tileQueue.messages)).To(Equal(tileQueueLenBefore + 1))
					tile := common.TileToProcess{}
					Expect(json.Unmarshal(tileQueue.messages[0], &tile)).NotTo(HaveOccurred())
					Expect(tile.ID).To(Equal(idb1))
				})
			})
		})
	})

	Describe("Getting Root tiles", func() {
		var roots []common.Tile
		BeforeEach(func() {
			_, _, _, idb0, idb1, _ := initDbScenesTiles(true)
			tileQueue.messages = nil
			sceneQueue.messages = nil
			wf.ResultHandler(ctx, common.Result{
				Type:    common.ResultTypeTile,
				ID:      idb0,
				Status:  common.StatusFAILED,
				Message: "error",
			})
			wf.ResultHandler(ctx, common.Result{
				Type:    common.ResultTypeTile,
				ID:      idb1,
				Status:  common.StatusFAILED,
				Message: "error",
			})

			roots, err = wf.RootTiles(ctx, aoi)
			Expect(err).NotTo(HaveOccurred())
		})
		It("should returns two tiles", func() {
			Expect(len(roots)).To(Equal(2))
			if roots[0].Scene.SourceID == "S1B_IW_SLC__1SDV_20180806T170022_20180806T170050_012145_0165D7_27D6" {
				expectTileToBeIngested(roots[0], rootSceneToIngest)
				expectTileToBeIngested(roots[1], leaf2SceneToIngest)
			} else {
				expectTileToBeIngested(roots[0], leaf2SceneToIngest)
				expectTileToBeIngested(roots[1], rootSceneToIngest)
			}
		})
	})

	Describe("Getting Leaf tiles", func() {
		var leaves []common.Tile
		BeforeEach(func() {
			_, _, _, idb0, idb1, _ := initDbScenesTiles(true)
			tileQueue.messages = nil
			sceneQueue.messages = nil
			wf.ResultHandler(ctx, common.Result{
				Type:    common.ResultTypeTile,
				ID:      idb0,
				Status:  common.StatusFAILED,
				Message: "error",
			})
			wf.ResultHandler(ctx, common.Result{
				Type:    common.ResultTypeTile,
				ID:      idb1,
				Status:  common.StatusFAILED,
				Message: "error",
			})

			leaves, err = wf.LeafTiles(ctx, aoi)
			Expect(err).NotTo(HaveOccurred())
		})
		It("should returns two leaf tiles", func() {
			Expect(len(leaves)).To(Equal(2))
			expectTileToBeIngested(leaves[0], leaf2SceneToIngest)
			expectTileToBeIngested(leaves[1], leaf2SceneToIngest)
		})
	})
})
