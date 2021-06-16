package main

import (
	"context"
	"fmt"
	"time"

	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/airbusgeo/geocube/interface/autoscaler"
	rc "github.com/airbusgeo/geocube/interface/autoscaler/k8s"
	"github.com/airbusgeo/geocube/interface/autoscaler/qbas"
	"github.com/airbusgeo/geocube/interface/messaging/pubsub"
	"go.uber.org/zap"
)

func runAutoscalers(ctx context.Context, project string, config autoscalerConfig) error {
	//image autoscaler
	ictx := log.WithFields(ctx, zap.String("rc", config.DownloaderRC), zap.String("queue", config.PsDownloaderQueue))

	controller, err := rc.New(config.DownloaderRC, config.Namespace)
	if err != nil {
		return fmt.Errorf("rc.new: %w", err)
	}
	controller.AllowEviction = false
	controller.CostPath = "/termination_cost"
	controller.CostPort = 9000

	queue, err := pubsub.NewConsumer(project, config.PsDownloaderQueue)
	if err != nil {
		return fmt.Errorf("pubsub.new: %w", err)
	}

	cfg := qbas.Config{
		Ratio:        2,
		MinRatio:     1,
		MaxInstances: config.MaxDownloaderInstances,
		MinInstances: 0,
		MaxStep:      1,
	}
	as := autoscaler.New(queue, controller, cfg, log.Logger(ictx))
	log.Logger(ictx).Sugar().Infof("starting autoscaler")
	go as.Run(ictx, 30*time.Second)

	//tile autoscaler
	bctx := log.WithFields(ctx, zap.String("rc", config.ProcessorRC), zap.String("queue", config.PsProcessorQueue))

	controller, err = rc.New(config.ProcessorRC, config.Namespace)
	if err != nil {
		return fmt.Errorf("rc.new: %w", err)
	}
	controller.AllowEviction = false
	controller.CostPath = "/termination_cost"
	controller.CostPort = 9000

	queue, err = pubsub.NewConsumer(project, config.PsProcessorQueue)
	if err != nil {
		return fmt.Errorf("pubsub.new: %w", err)
	}

	cfg = qbas.Config{
		Ratio:        1.2,
		MinRatio:     1,
		MaxInstances: config.MaxProcessorInstances,
		MinInstances: 0,
		MaxStep:      5,
	}
	as = autoscaler.New(queue, controller, cfg, log.Logger(bctx))
	log.Logger(bctx).Sugar().Infof("starting autoscaler")
	go as.Run(bctx, 30*time.Second)
	return nil
}
