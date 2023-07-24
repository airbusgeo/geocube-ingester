package annotations

import (
	"encoding/xml"
	"fmt"
	"math"
)

type Burst struct {
	SwathID     string
	TileNr      int
	AnxTime     int
	GeometryWKT string
}

func BurstsFromAnnotation(annotationFile []byte, annotationURL string) (map[int]*Burst, error) {
	// XML GridPoint structure
	type GridPoint struct {
		Pixel     int     `xml:"pixel"`
		Line      int     `xml:"line"`
		Latitude  float64 `xml:"latitude"`
		Longitude float64 `xml:"longitude"`
	}

	// Read annotations file
	annotation := struct {
		XMLName         xml.Name  `xml:"product"`
		Swath           string    `xml:"adsHeader>swath"`
		LinesPerBurst   int       `xml:"swathTiming>linesPerBurst"`
		SamplesPerBurst int       `xml:"swathTiming>samplesPerBurst"`
		AzimuthAnxTime  []float64 `xml:"swathTiming>burstList>burst>azimuthAnxTime"`

		GridPoint []GridPoint `xml:"geolocationGrid>geolocationGridPointList>geolocationGridPoint"`
	}{}
	if err := xml.Unmarshal(annotationFile, &annotation); err != nil {
		return nil, fmt.Errorf("readAnnotation.Unmarshal[%s] : %w", annotationFile, err)
	}

	// Position of firsts and last points
	first := map[int]GridPoint{}
	last := map[int]GridPoint{}
	for _, point := range annotation.GridPoint {
		if point.Pixel == 0 {
			first[point.Line] = point
		} else if point.Pixel == annotation.SamplesPerBurst-1 {
			last[point.Line] = point
		}
	}

	// Burst
	bursts := map[int]*Burst{}
	for i, anxTime := range annotation.AzimuthAnxTime {
		// First/Last lines of the burst
		firstline := i * annotation.LinesPerBurst
		lastline := (i + 1) * annotation.LinesPerBurst
		if _, ok := first[firstline]; !ok {
			firstline-- // -1 because first and lastline sometimes shifts by 1 for some reason?
			if _, ok := first[firstline]; !ok {
				return nil, fmt.Errorf("readAnnotation: First line not found in annotation file %s", annotationURL)
			}
		}
		if _, ok := last[lastline]; !ok {
			lastline-- // -1 because first and lastline sometimes shifts by 1 for some reason?
			if _, ok := last[lastline]; !ok {
				return nil, fmt.Errorf("readAnnotation: Last line not found in annotation file %s", annotationURL)
			}
		}

		// Set bursts
		intAnxTime := int(math.Round(math.Mod(anxTime, float64(12*24*60*60/175)) * 10))
		bursts[intAnxTime] = &Burst{
			SwathID: annotation.Swath,
			TileNr:  i + 1,
			AnxTime: intAnxTime,
			GeometryWKT: fmt.Sprintf("POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
				first[firstline].Longitude, first[firstline].Latitude,
				first[lastline].Longitude, first[lastline].Latitude,
				last[lastline].Longitude, last[lastline].Latitude,
				last[firstline].Longitude, last[firstline].Latitude,
				first[firstline].Longitude, first[firstline].Latitude,
			),
		}
	}

	return bursts, nil
}
