package main

import (
	"context"
	"fmt"
	"time"

	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/airbusgeo/geocube/interface/autoscaler"
	rc "github.com/airbusgeo/geocube/interface/autoscaler/k8s"
	"github.com/airbusgeo/geocube/interface/autoscaler/qbas"
)

func runAutoscalers(ctx context.Context, downloaderBacklog, processorBacklog qbas.Queue, config autoscalerConfig) error {
	// downloader autoscaler
	ictx := log.With(ctx, "rc", config.DownloaderRC)

	controller, err := rc.New(config.DownloaderRC, config.Namespace)
	if err != nil {
		return fmt.Errorf("rc.new: %w", err)
	}
	controller.AllowEviction = false
	controller.CostPath = "/termination_cost"
	controller.CostPort = 9000

	cfg := qbas.Config{
		Ratio:        2,
		MinRatio:     1,
		MaxInstances: config.MaxDownloaderInstances,
		MinInstances: 0,
		MaxStep:      1,
	}
	as := autoscaler.New(downloaderBacklog, controller, cfg, log.Logger(ictx))
	log.Logger(ictx).Sugar().Infof("starting autoscaler")
	go as.Run(ictx, 30*time.Second)

	// processor autoscaler
	bctx := log.With(ctx, "rc", config.ProcessorRC)

	controller, err = rc.New(config.ProcessorRC, config.Namespace)
	if err != nil {
		return fmt.Errorf("rc.new: %w", err)
	}
	controller.AllowEviction = false
	controller.CostPath = "/termination_cost"
	controller.CostPort = 9000

	cfg = qbas.Config{
		Ratio:        1.2,
		MinRatio:     1,
		MaxInstances: config.MaxProcessorInstances,
		MinInstances: 0,
		MaxStep:      40,
	}
	as = autoscaler.New(processorBacklog, controller, cfg, log.Logger(bctx))
	log.Logger(bctx).Sugar().Infof("starting autoscaler")
	go as.Run(bctx, 30*time.Second)
	return nil
}
