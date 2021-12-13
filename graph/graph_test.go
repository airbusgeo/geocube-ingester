package graph_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"

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
		Condition: graph.ConditionT0T1,

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

	Describe("Loading tile condition", func() {
		var final_condition, expected_condition graph.TileCondition
		var itShouldBeEqual = func() {
			It("should be equal", func() {
				Expect(final_condition.Name).To(Equal(expected_condition.Name))
			})
		}

		JustBeforeEach(func() {
			stepb, err := json.Marshal(&expected_condition)
			Expect(err).NotTo(HaveOccurred())
			err = json.Unmarshal(stepb, &final_condition)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("Pass", func() {
			BeforeEach(func() {
				expected_condition = graph.TileCondition(graph.ConditionPass)
			})
			itShouldBeEqual()
		})

		Context("T0T1", func() {
			BeforeEach(func() {
				expected_condition = graph.TileCondition(graph.ConditionT0T1)
			})
			itShouldBeEqual()
		})

		Context("T1T2", func() {
			BeforeEach(func() {
				expected_condition = graph.TileCondition(graph.ConditionT1T2)
			})
			itShouldBeEqual()
		})

		Context("T0T2", func() {
			BeforeEach(func() {
				expected_condition = graph.TileCondition(graph.ConditionT0T2)
			})
			itShouldBeEqual()
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
				Expect(final_graph.InFiles).To(Equal(expected_graph.InFiles))
				Expect(final_graph.OutFiles).To(Equal(expected_graph.OutFiles))
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
						{{graph.File{service.LayerPreprocessed, service.ExtensionDIMAP}, false}},
						{},
						{{graph.File{service.LayerPreprocessed, service.ExtensionDIMAP}, false}},
					},
					OutFiles: [][]graph.OutFile{
						{
							{File: graph.File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: graph.ToCreate},
							{File: graph.File{Layer: service.LayerPreprocessed, Extension: service.ExtensionDIMAP}, Action: graph.ToDelete},
						},
						{
							{File: graph.File{Layer: service.LayerCoregExtract, Extension: service.ExtensionDIMAP}, Action: graph.ToDelete},
						},
						{},
					},
					OutfilesCond: [][]graph.TileCondition{
						{graph.ConditionT0T1, graph.ConditionT1T2},
						{graph.ConditionT0T1},
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
			byteValue, _ := ioutil.ReadAll(jsonFile)
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
