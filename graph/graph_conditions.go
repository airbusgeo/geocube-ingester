package graph

import (
	"errors"
	"os"

	"github.com/airbusgeo/geocube-ingester/common"
	"github.com/airbusgeo/geocube-ingester/service"
)

// Condition to do an action
type Condition struct {
	Name   string
	PassFn interface{}
}

// TileCondition is a condition on tiles to do an action (execute a step, create a file...)
type TileCondition Condition

// TileConditionFn is a PassFunction of TileCondition executed to test a condition on tiles
type TileConditionFn func([]common.Tile) bool

// pass is a condition always true
var pass = TileCondition{"pass", TileConditionFn(func(tiles []common.Tile) bool { return true })}

// condDiffTile returns true if tile1 != tile2
var condDiffT0T1 = TileCondition{"different_T0_T1", TileConditionFn(func(tiles []common.Tile) bool { return tiles[0].Scene.SourceID != tiles[1].Scene.SourceID })}
var condDiffT0T2 = TileCondition{"different_T0_T2", TileConditionFn(func(tiles []common.Tile) bool { return tiles[0].Scene.SourceID != tiles[2].Scene.SourceID })}
var condDiffT1T2 = TileCondition{"different_T1_T2", TileConditionFn(func(tiles []common.Tile) bool { return tiles[1].Scene.SourceID != tiles[2].Scene.SourceID })}

// condEqualTile returns true if tile1 == tile2
var condEqualT0T1 = TileCondition{"equal_T0_T1", TileConditionFn(func(tiles []common.Tile) bool { return tiles[0].Scene.SourceID == tiles[1].Scene.SourceID })}
var condEqualT0T2 = TileCondition{"equal_T0_T2", TileConditionFn(func(tiles []common.Tile) bool { return tiles[0].Scene.SourceID == tiles[2].Scene.SourceID })}
var condEqualT1T2 = TileCondition{"equal_T1_T2", TileConditionFn(func(tiles []common.Tile) bool { return tiles[1].Scene.SourceID == tiles[2].Scene.SourceID })}

var tileConditionJSON = map[string]TileCondition{
	pass.Name:          pass,
	condDiffT0T1.Name:  condDiffT0T1,
	condDiffT0T2.Name:  condDiffT0T2,
	condDiffT1T2.Name:  condDiffT1T2,
	condEqualT0T1.Name: condEqualT0T1,
	condEqualT0T2.Name: condEqualT0T2,
	condEqualT1T2.Name: condEqualT1T2,
}

// ErrorConditionFn is a PassFunction of FileCondition executed in case of error
type ErrorConditionFn func(error) bool

// FileConditionFn is a PassFunction of FileCondition executed at the end of processing steps
type FileConditionFn func(common.Tile, *File) bool

// condOnFailure & condOnFatalFailure returns true if an error or a fatal error occured
var condOnFailure = Condition{"on_failure", ErrorConditionFn(func(err error) bool { return err != nil })}
var condOnFatalFailure = Condition{"on_fatal_failure", ErrorConditionFn(func(err error) bool { return service.Fatal(err) })}

// condIfExists return true if file exists
var condFileExists = Condition{"file_exists", FileConditionFn(func(t common.Tile, f *File) bool {
	if f != nil {
		_, err := os.Stat(service.LayerFileName(t, f.Layer, f.Extension))
		return err == nil
	}
	return false
})}

// condIfNotExists return true if file does not exist
var condFileNotExist = Condition{"file_not_exist", FileConditionFn(func(t common.Tile, f *File) bool {
	if f != nil {
		_, err := os.Stat(service.LayerFileName(t, f.Layer, f.Extension))
		return err != nil && errors.Is(err, os.ErrNotExist)
	}
	return false
})}

var conditionJSON = map[string]Condition{
	condFileExists.Name:     condFileExists,
	condFileNotExist.Name:   condFileNotExist,
	condOnFailure.Name:      condOnFailure,
	condOnFatalFailure.Name: condOnFatalFailure,
	pass.Name:               Condition(pass),
	condDiffT0T1.Name:       Condition(condDiffT0T1),
	condDiffT0T2.Name:       Condition(condDiffT0T2),
	condDiffT1T2.Name:       Condition(condDiffT1T2),
	condEqualT0T1.Name:      Condition(condEqualT0T1),
	condEqualT0T2.Name:      Condition(condEqualT0T2),
	condEqualT1T2.Name:      Condition(condEqualT1T2),
}

func (t TileCondition) Pass(tiles []common.Tile) bool {
	return t.PassFn.(TileConditionFn)(tiles)
}
