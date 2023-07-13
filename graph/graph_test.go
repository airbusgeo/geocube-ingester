package graph_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/graph"
	"github.com/airbusgeo/geocube-ingester/service"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LoadGraph", func() {

	snapStep := graph.ProcessingStep{
		Engine:    "snap",
		Command:   "snap_graph.xml",
		Condition: graph.ConditionPass,

		Args: map[string]graph.Arg{
			"input":  graph.ArgTile("scene"),
			"output": graph.ArgOut{service.LayerPreprocessed, service.ExtensionDIMAP},
			"swath":  graph.ArgTile("swath"),
			"polar":  graph.ArgFixed("\"VV VH\""),
			"burst":  graph.ArgTile("number"),
		},
	}

	pythonStep := graph.ProcessingStep{
		Engine:    "python",
		Command:   "python_command.py",
		Condition: graph.ConditionDiffT0T1,

		Args: map[string]graph.Arg{
			"file-in":    graph.ArgOut{service.LayerCoherenceVH, service.ExtensionGTiff},
			"file-out":   graph.ArgOut{service.LayerCoherenceVH, service.ExtensionGTiff},
			"no-data":    graph.ArgFixed("0"),
			"iterations": graph.ArgConfig("coh_erode_iterations"),
		},
	}

	var stepsShouldBeEqual = func(final_step, expected_step graph.ProcessingStep) {
		Expect(final_step.Engine).To(Equal(expected_step.Engine))
		Expect(final_step.Command).To(Equal(expected_step.Command))
		Expect(final_step.Args).To(Equal(expected_step.Args))
		Expect(final_step.Condition.Name).To(Equal(expected_step.Condition.Name))
	}
	var inFilesShouldBeEqual = func(final_infile, expected_infile graph.InFile) {
		final_infile.Condition.PassFn = nil
		expected_infile.Condition.PassFn = nil
		Expect(final_infile).To(Equal(expected_infile))
	}
	var outFilesShouldBeEqual = func(final_outfile, expected_outfile graph.OutFile) {
		final_outfile.Condition.PassFn = nil
		expected_outfile.Condition.PassFn = nil
		final_outfile.ErrorCondition.PassFn = nil
		expected_outfile.ErrorCondition.PassFn = nil
		Expect(final_outfile).To(Equal(expected_outfile))
	}

	Describe("Loading tile condition", func() {
		var final_condition, expected_condition graph.TileCondition
		var unmarshalErr error
		var itShouldBeEqual = func() {
			It("should be equal", func() {
				Expect(final_condition.Name).To(Equal(expected_condition.Name))
			})
		}

		var itShouldNotRaiseError = func() {
			It("should raise error", func() {
				Expect(unmarshalErr).To(BeNil())
			})
		}

		var itShouldRaiseError = func() {
			It("should raise error", func() {
				Expect(unmarshalErr).NotTo(BeNil())
			})
		}

		JustBeforeEach(func() {
			stepb, err := json.Marshal(&expected_condition)
			Expect(err).NotTo(HaveOccurred())
			unmarshalErr = json.Unmarshal(stepb, &final_condition)
		})

		Context("Pass", func() {
			BeforeEach(func() {
				expected_condition = graph.ConditionPass
			})
			itShouldNotRaiseError()
			itShouldBeEqual()
		})

		Context("T0T1", func() {
			BeforeEach(func() {
				expected_condition = graph.ConditionDiffT0T1
			})
			itShouldNotRaiseError()
			itShouldBeEqual()
		})

		Context("T1T2", func() {
			BeforeEach(func() {
				expected_condition = graph.ConditionDiffT1T2
			})
			itShouldNotRaiseError()
			itShouldBeEqual()
		})

		Context("T0T2", func() {
			BeforeEach(func() {
				expected_condition = graph.ConditionDiffT0T2
			})
			itShouldNotRaiseError()
			itShouldBeEqual()
		})

		Context("FatalError", func() {
			BeforeEach(func() {
				expected_condition = graph.TileCondition(graph.ConditionOnFatalFailure)
			})
			itShouldRaiseError()
		})
	})

	Describe("Loading argument", func() {
		var final_arg, expected_arg graph.Arg
		var itShouldBeEqual = func() {
			It("should be equal", func() {
				Expect(expected_arg).To(Equal(final_arg))
			})
		}

		JustBeforeEach(func() {
			stepb, err := json.Marshal(&expected_arg)
			Expect(err).NotTo(HaveOccurred())
			var argJson graph.ArgJSON
			err = json.Unmarshal(stepb, &argJson)
			Expect(err).NotTo(HaveOccurred())
			final_arg = argJson.Arg
		})

		Context("ArgFixed", func() {
			BeforeEach(func() {
				expected_arg = graph.ArgFixed("fixed_arg")
			})
			itShouldBeEqual()
		})

		Context("ArgConfig", func() {
			BeforeEach(func() {
				expected_arg = graph.ArgConfig("config_flag")
			})
			itShouldBeEqual()
		})

		Context("ArgTile", func() {
			BeforeEach(func() {
				expected_arg = graph.ArgTile("tile_flag")
			})
			itShouldBeEqual()
		})

		Context("ArgIn", func() {
			BeforeEach(func() {
				expected_arg = graph.ArgIn{Input: 2, Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}
			})
			itShouldBeEqual()
		})

		Context("ArgOut", func() {
			BeforeEach(func() {
				expected_arg = graph.ArgOut{service.LayerCoregistrated, service.ExtensionDIMAP}
			})
			itShouldBeEqual()
		})
	})

	Describe("Loading step", func() {
		var final_step, expected_step graph.ProcessingStep

		JustBeforeEach(func() {
			stepb, err := json.Marshal(&expected_step)
			Expect(err).NotTo(HaveOccurred())
			err = json.Unmarshal(stepb, &final_step)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("snap step", func() {
			BeforeEach(func() {
				expected_step = snapStep
			})
			It("should be equal", func() {
				stepsShouldBeEqual(final_step, expected_step)
			})
		})

		Context("python step", func() {
			BeforeEach(func() {
				expected_step = pythonStep
			})
			It("should be equal", func() {
				stepsShouldBeEqual(final_step, expected_step)
			})
		})
	})

	Describe("Loading graph", func() {
		var final_graph, expected_graph graph.ProcessingGraphJSON
		var itShouldBeEqual = func() {
			It("should be equal", func() {
				Expect(final_graph.Config).To(Equal(expected_graph.Config))
				for i, infiles := range final_graph.InFiles {
					for j, infile := range infiles {
						inFilesShouldBeEqual(infile, expected_graph.InFiles[i][j])
					}
				}
				for i, outfiles := range final_graph.OutFiles {
					for j, outfile := range outfiles {
						outFilesShouldBeEqual(outfile, expected_graph.OutFiles[i][j])
					}
				}
				Expect(len(final_graph.Steps)).To(Equal(len(expected_graph.Steps)))
				for i, step := range final_graph.Steps {
					stepsShouldBeEqual(step, final_graph.Steps[i])
				}
			})
		}

		JustBeforeEach(func() {
			stepb, err := json.Marshal(&expected_graph)
			Expect(err).NotTo(HaveOccurred())
			err = json.Unmarshal(stepb, &final_graph)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("", func() {
			BeforeEach(func() {
				expected_graph = graph.ProcessingGraphJSON{
					Steps: []graph.ProcessingStep{snapStep, pythonStep},
					InFiles: [3][]graph.InFile{
						{{graph.File{service.LayerPreprocessed, service.ExtensionDIMAP}, graph.Condition(graph.ConditionPass)}},
						{},
						{{graph.File{service.LayerPreprocessed, service.ExtensionDIMAP}, graph.Condition(graph.ConditionDiffT0T2)}},
					},
					OutFiles: [][]graph.OutFile{
						{
							{File: graph.File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: graph.ToCreate, Condition: graph.Condition(graph.ConditionDiffT0T1), ErrorCondition: graph.Condition(graph.ConditionPass)},
							{File: graph.File{Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}, Action: graph.ToDelete, Condition: graph.Condition(graph.ConditionDiffT1T2), ErrorCondition: graph.Condition(graph.ConditionPass)},
						},
						{
							{File: graph.File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: graph.ToDelete, Condition: graph.Condition(graph.ConditionDiffT0T1), ErrorCondition: graph.Condition(graph.ConditionPass)},
						},
						{},
					},
				}
			})
			itShouldBeEqual()
		})
	})

	Context("Loading graph", func() {
		var (
			jsonPath    string
			final_graph graph.ProcessingGraphJSON
			err         error
			wd, _       = os.Getwd()
		)
		var itShouldNotRaiseError = func() {
			It("should not raise error", func() {
				Expect(err).To(BeNil())
			})
		}

		JustBeforeEach(func() {
			var jsonFile *os.File
			jsonFile, err = os.Open(path.Join(wd, jsonPath))
			Expect(err).NotTo(HaveOccurred())
			byteValue, _ := io.ReadAll(jsonFile)
			err = json.Unmarshal(byteValue, &final_graph)
		})

		Describe("S1CoregExtract", func() {
			BeforeEach(func() {
				jsonPath = "library/S1CoregExtract.json"
			})
			itShouldNotRaiseError()
		})

		Describe("S1BackscatterCoherence", func() {
			BeforeEach(func() {
				jsonPath = "library/S1BackscatterCoherence.json"
			})
			itShouldNotRaiseError()
		})

		Describe("CopyProductToStorage", func() {
			BeforeEach(func() {
				jsonPath = "library/CopyProductToStorage.json"
			})
			itShouldNotRaiseError()
		})

		Describe("Pass", func() {
			BeforeEach(func() {
				jsonPath = "library/Pass.json"
			})
			itShouldNotRaiseError()
		})
	})
})

var _ = Describe("ExecuteGraph", func() {

	Describe("Executing graph", func() {
		var ctx = context.Background()
		var processing_graph graph.ProcessingGraphJSON
		var returned_err error
		var outFiles [][]graph.OutFile

		var itPassWithoutError = func() {
			It("should pass without error", func() {
				Expect(returned_err).To(BeNil())
			})
		}
		var itRaiseAFatalError = func() {
			It("should raise a fatal error", func() {
				Expect(returned_err).NotTo(BeNil())
				Expect(service.Fatal(returned_err)).To(BeTrue())
			})
		}

		var itAsksToDeleteFiles = func(to_delete []bool) {
			It("should asks to delete some files", func() {
				for i, to_delete := range to_delete {
					if to_delete {
						Expect(len(outFiles[i])).To(Equal(1))
						Expect(outFiles[i][0].Action).To(Equal(graph.ToDelete))
					} else {
						Expect(len(outFiles[i])).To(Equal(0))
					}
				}
			})
		}

		JustBeforeEach(func() {
			p_graph, err := graph.NewProcessingGraph(ctx, processing_graph.Steps, processing_graph.InFiles, processing_graph.OutFiles)
			Expect(err).NotTo(HaveOccurred())
			outFiles, returned_err = p_graph.Process(ctx, graph.GraphConfig{}, graph.GraphEnvs{}, []common.Tile{
				{Scene: common.Scene{SourceID: "1"}},
				{Scene: common.Scene{SourceID: "1"}},
				{Scene: common.Scene{SourceID: "2"}}})
		})

		Context("Pass", func() {
			BeforeEach(func() {
				processing_graph = graph.ProcessingGraphJSON{
					Steps: []graph.ProcessingStep{},
					InFiles: [3][]graph.InFile{
						{},
						{},
						{},
					},
					OutFiles: [][]graph.OutFile{
						{{Action: graph.ToDelete, Condition: graph.ConditionOnFatalFailure}},
						{},
						{},
					},
				}
			})
			itPassWithoutError()
			itAsksToDeleteFiles([]bool{false})
		})

		Context("Raise a Fatal Error", func() {
			BeforeEach(func() {
				processing_graph = graph.ProcessingGraphJSON{
					Steps: []graph.ProcessingStep{
						{
							Engine:    "python",
							Command:   "python/fatal_error.py",
							Condition: graph.ConditionPass,

							Args: map[string]graph.Arg{},
						},
					},
					InFiles: [3][]graph.InFile{
						{},
						{},
						{},
					},
					OutFiles: [][]graph.OutFile{
						{{Action: graph.ToDelete, Condition: graph.ConditionOnFatalFailure}},
						{{Action: graph.ToDelete, Condition: graph.Condition(graph.ConditionDiffT0T1), ErrorCondition: graph.ConditionOnFatalFailure}},
						{{Action: graph.ToDelete, Condition: graph.Condition(graph.ConditionDiffT0T2), ErrorCondition: graph.ConditionOnFatalFailure}},
					},
				}
			})
			itRaiseAFatalError()
			itAsksToDeleteFiles([]bool{true, false, true})
		})
	})
})
