# Sensors

## Add a new sensor

1. Add the name of the constellation (or the name of the sensor) to the list of `Constellation` in `common/naming.go`.
2. Call `go generate ./...`
3. Update the functions of `common/naming.go`.
4. In `catalog/catalog.go`, adapt `DoTilesInventory` method in order to be able to interpret the new constellation.

The next step might be to add [a catalogue provider](catalog.md#add-a-new-catalogue) or an [image provider](providers.md#add-a-new-provider) for this sensor.
