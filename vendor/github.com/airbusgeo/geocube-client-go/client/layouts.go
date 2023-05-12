package client

import (
	"context"
	"fmt"
	"io"
	"strings"

	pb "github.com/airbusgeo/geocube-client-go/pb"
)

var MUCOGPattern = "Z=0>T>R>B;R>Z=1:>T>B"
var COGPattern = "R>Z=1:>T>B;R>Z=0>T>B"

type Layout pb.Layout

type Tile struct {
	Transform     [6]float64
	CRS           string
	Width, Height int32
	err           error
}

// TileError is an erroneous tile
func TileError(err error) Tile {
	return Tile{err: err}
}

// Error returns an error if the tile is an erroneous tile
func (t *Tile) Error() error {
	return t.err
}

func NewTileFromPb(pbt *pb.Tile) *Tile {
	return &Tile{
		Transform: [6]float64{pbt.Transform.A, pbt.Transform.B, pbt.Transform.C, pbt.Transform.D, pbt.Transform.E, pbt.Transform.F},
		CRS:       pbt.Crs,
		Width:     pbt.SizePx.Width,
		Height:    pbt.SizePx.Height,
	}
}

func NewRegularLayout(name, crs string, resolution float64, sizeXPx, sizeYPx, originX, originY, blockXSize, blockYSize, maxRecords, overviewsMinSize int64, interlacingPattern string) Layout {
	if blockXSize == -1 {
		blockXSize = 256
	}
	if blockYSize == -1 {
		blockYSize = 256
	}
	if maxRecords == -1 {
		maxRecords = 1000
	}

	return Layout{
		Name:      name,
		GridFlags: []string{},
		GridParameters: map[string]string{
			"grid":        "regular",
			"crs":         crs,
			"resolution":  fmt.Sprintf("%v", resolution),
			"cell_x_size": fmt.Sprintf("%v", sizeXPx),
			"cell_y_size": fmt.Sprintf("%v", sizeYPx),
			"ox":          fmt.Sprintf("%v", originX),
			"oy":          fmt.Sprintf("%v", originY),
		},
		BlockXSize:         blockXSize,
		BlockYSize:         blockYSize,
		MaxRecords:         maxRecords,
		OverviewsMinSize:   overviewsMinSize,
		InterlacingPattern: interlacingPattern,
	}
}

func (c Client) CreateLayout(ctx context.Context, name string, gridFlags []string, gridParameters map[string]string, blockXSize, blockYSize, maxRecords, overviewsMinSize int64, interlacingPattern string) error {
	if _, err := c.gcc.CreateLayout(ctx,
		&pb.CreateLayoutRequest{Layout: &pb.Layout{
			Name:               name,
			GridFlags:          gridFlags,
			GridParameters:     gridParameters,
			BlockXSize:         blockXSize,
			BlockYSize:         blockYSize,
			MaxRecords:         maxRecords,
			OverviewsMinSize:   overviewsMinSize,
			InterlacingPattern: interlacingPattern}}); err != nil {
		return grpcError(err)
	}

	return nil
}

func (c Client) ListLayouts(ctx context.Context, nameLike string) ([]*Layout, error) {
	resp, err := c.gcc.ListLayouts(ctx, &pb.ListLayoutsRequest{NameLike: nameLike})

	if err != nil {
		return nil, grpcError(err)
	}

	var layouts []*Layout
	for _, l := range resp.Layouts {
		layouts = append(layouts, (*Layout)(l))
	}

	return layouts, nil
}

func (c Client) TileAOI(ctx context.Context, aoi AOI, layoutName string, layout *Layout) (<-chan Tile, error) {
	req := &pb.TileAOIRequest{Aoi: pbFromAOI(aoi)}
	if layout != nil {
		req.Identifier = &pb.TileAOIRequest_Layout{Layout: (*pb.Layout)(layout)}
	} else if layoutName != "" {
		req.Identifier = &pb.TileAOIRequest_LayoutName{LayoutName: layoutName}
	} else {
		return nil, fmt.Errorf("TileAOI: either layoutName or layout must be specified")
	}

	stream, err := c.gcc.TileAOI(ctx, req)
	if err != nil {
		return nil, grpcError(err)
	}

	tiles := make(chan Tile)
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				tiles <- TileError(err)
				break
			}
			for _, tile := range resp.Tiles {
				tiles <- *NewTileFromPb(tile)
			}
		}
		close(tiles)
	}()

	return tiles, nil
}

// ToString returns a string with a representation of the layout
func (l *Layout) ToString() string {
	s := fmt.Sprintf("Layout %s:\n"+
		"  Block XSize:         %d\n"+
		"  Block YSize:         %d\n"+
		"  Max records:         %d\n"+
		"  OverviewsMinSize:    %d\n"+
		"  Interlacing pattern: %s\n"+
		"  Grid flags:          %s\n"+
		"  Grid parameters:\n",
		l.Name, l.BlockXSize, l.BlockYSize, l.MaxRecords, l.OverviewsMinSize, l.InterlacingPattern,
		strings.Join(l.GridFlags, " "))
	appendDict(l.GridParameters, &s)
	return s
}
