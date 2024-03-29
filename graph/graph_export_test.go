package graph

import (
	"encoding/json"
	"fmt"
)

var ConditionPass = pass
var ConditionDiffT0T1 = condDiffT0T1
var ConditionDiffT1T2 = condDiffT1T2
var ConditionDiffT0T2 = condDiffT0T2
var ConditionOnFatalFailure = condOnFatalFailure

var NewS1PreProcessingGraph = newS1PreProcessingGraph

var NewOutFile = newOutFile

func (tc TileCondition) MarshalJSON() ([]byte, error) {
	return json.Marshal(tc.Name)
}

func (fc Condition) MarshalJSON() ([]byte, error) {
	return json.Marshal(fc.Name)
}

type argJSON struct {
	Type      string `json:"type"`
	Value     string `json:"value"`
	Input     int    `json:"tile_index"`
	Layer     string `json:"layer"`
	Extension string `json:"extension"`
}

func (dtype DType) MarshalJSON() ([]byte, error) {
	var s string
	switch dtype {
	case UInt8:
		s = "uint8"
	case UInt16:
		s = "uint16"
	case UInt32:
		s = "uint32"
	case Int16:
		s = "int16"
	case Int32:
		s = "int32"
	case Float32:
		s = "float32"
	case Float64:
		s = "float64"
	case Complex64:
		s = "complex64"
	default:
		s = "undefined"
	}

	return json.Marshal(s)
}

func (a ArgFixed) MarshalJSON() ([]byte, error) {
	return json.Marshal(argJSON{Type: "fixed", Value: string(a)})
}

func (a ArgConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(argJSON{Type: "config", Value: string(a)})
}

func (a ArgTile) MarshalJSON() ([]byte, error) {
	return json.Marshal(argJSON{Type: "tile", Value: string(a)})
}

func (a ArgIn) MarshalJSON() ([]byte, error) {
	return json.Marshal(argJSON{Type: "in", Input: a.Input, Layer: string(a.Layer), Extension: string(a.Extension)})
}

func (a ArgOut) MarshalJSON() ([]byte, error) {
	return json.Marshal(argJSON{Type: "out", Layer: string(a.Layer), Extension: string(a.Extension)})
}

func (a OutFileAction) MarshalJSON() ([]byte, error) {
	var action string
	switch a {
	case ToIgnore:
		action = "to_ignore"
	case ToCreate:
		action = "to_create"
	case ToIndex:
		action = "to_index"
	case ToDelete:
		action = "to_delete"
	default:
		return nil, fmt.Errorf("unknown action: %v", a)
	}
	return json.Marshal(action)
}

func (s ProcessingStep) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Engine    string         `json:"engine"` // Python or Snap
		Command   string         `json:"command"`
		Args      map[string]Arg `json:"args"`
		Condition TileCondition  `json:"condition"`
	}{s.Engine, s.Command, s.Args, TileCondition(s.Condition)})
}
