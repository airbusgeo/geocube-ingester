package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/airbusgeo/geocube-ingester/graph"
)

func main() {
	ctx := context.Background()

	os.Setenv("GRAPHPATH", ".")

	graph_path := flag.String("path", "", "json graph path")
	flag.Parse()

	g, conf, _, err := graph.LoadGraphFromFile(ctx, *graph_path)
	if err != nil {
		log.Fatal(err)
	}

	s := fmt.Sprintf("Load %s:\n%s\n- config:\n", *graph_path, g.Summary())
	for k, v := range conf {
		s += fmt.Sprintf("  * %-25s: %s\n", k, v)
	}
	log.Print(s)
}
