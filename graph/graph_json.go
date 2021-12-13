package graph

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type ProcessingGraphJSON struct {
	Config       map[string]string `json:"config"`
	Steps        []ProcessingStep  `json:"processing_steps"`
	InFiles      [3][]InFile       `json:"in_files"`
	OutFiles     [][]OutFile       `json:"out_files"`
	OutfilesCond [][]TileCondition `json:"out_files_conditions"`
}

var tileConditionJSON = map[string]TileCondition{
	pass.Name:         pass,
	condDiffT0T1.Name: condDiffT0T1,
	condDiffT0T2.Name: condDiffT0T2,
	condDiffT1T2.Name: condDiffT1T2,
}

func (t *TileCondition) UnmarshalJSON(data []byte) error {
	var res string
	if err := json.Unmarshal(data, &res); err != nil {
		return err
	}

	var ok bool
	*t, ok = tileConditionJSON[res]
	if !ok {
		return fmt.Errorf("UnmarshalJSON: unknown condition: %s (must be one of %v)", res, reflect.ValueOf(tileConditionJSON).MapKeys())
	}
	return nil
}

type ArgJSON struct {
	Arg
}

func (a *ArgJSON) UnmarshalJSON(data []byte) error {
	res := struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}{}
	if err := json.Unmarshal(data, &res); err != nil {
		return err
	}

	switch res.Type {
	case "fixed":
		a.Arg = ArgFixed(res.Value)
	case "config":
		a.Arg = ArgConfig(res.Value)
	case "tile":
		a.Arg = ArgTile(res.Value)
	case "in":
		var in ArgIn
		if err := json.Unmarshal(data, &in); err != nil {
			return err
		}
		a.Arg = in
	case "out":
		var out ArgOut
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		a.Arg = out
	default:
		return fmt.Errorf("UnmarshalJSON: unknown type: %s (must be one of fixed, config, tile, tile_in, out)", res)
	}
	return nil
}

func (a *ProcessingStep) UnmarshalJSON(data []byte) error {
	res := struct {
		Engine    string             `json:"engine"` // Python or Snap
		Command   string             `json:"command"`
		Args      map[string]ArgJSON `json:"args"`
		Condition TileCondition      `json:"condition"`
	}{}
	if err := json.Unmarshal(data, &res); err != nil {
		return err
	}

	*a = ProcessingStep{
		Engine:    res.Engine,
		Command:   res.Command,
		Args:      map[string]Arg{},
		Condition: res.Condition,
	}
	for k, v := range res.Args {
		a.Args[k] = v.Arg
	}
	return nil
}

func (a *OutFileAction) UnmarshalJSON(data []byte) error {
	var action string
	if err := json.Unmarshal(data, &action); err != nil {
		return err
	}
	switch action {
	case "to_ignore":
		*a = ToIgnore
	case "to_create":
		*a = ToCreate
	case "to_index":
		*a = ToIndex
	case "to_delete":
		*a = ToDelete
	default:
		return fmt.Errorf("unknown action: %v", data)
	}
	return nil
}
