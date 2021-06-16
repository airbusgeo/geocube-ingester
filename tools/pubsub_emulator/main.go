package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var eventsTopic = "ingester-events"
var downloaderTopic = "ingester-downloader"
var processorTopic = "ingester-processor"

var eventsSubscription = "ingester-events"
var downloaderSubscription = "ingester-downloader"
var processorSubscription = "ingester-processor"

func main() {
	ctx := context.Background()

	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	projectID := flag.String("project", "geocube-emulator", "emulator project")
	flag.Parse()

	log.Print("New client for project " + *projectID)
	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}

	log.Print("Create Topic : " + eventsTopic)
	if _, err = client.CreateTopic(ctx, eventsTopic); err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("pubsub.CreateTopic: %v", err)
	}

	log.Print("Create Topic : " + downloaderTopic)
	if _, err = client.CreateTopic(ctx, downloaderTopic); err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("pubsub.CreateTopic: %v", err)
	}

	log.Print("Create Topic : " + processorTopic)
	if _, err = client.CreateTopic(ctx, processorTopic); err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("pubsub.CreateTopic: %v", err)
	}

	log.Print("Create Subscription : " + eventsSubscription)
	if _, err = client.CreateSubscription(ctx, eventsSubscription, pubsub.SubscriptionConfig{
		Topic:       client.Topic(eventsTopic),
		AckDeadline: 10 * time.Second,
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("CreateSubscription: %v", err)
	}

	log.Print("Create Subscription : " + downloaderSubscription)
	if _, err = client.CreateSubscription(ctx, downloaderSubscription, pubsub.SubscriptionConfig{
		Topic:       client.Topic(downloaderTopic),
		AckDeadline: 10 * time.Second,
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("CreateSubscription: %v", err)
	}

	log.Print("Create Subscription : " + processorSubscription)
	if _, err = client.CreateSubscription(ctx, processorSubscription, pubsub.SubscriptionConfig{
		Topic:       client.Topic(processorTopic),
		AckDeadline: 10 * time.Second,
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("CreateSubscription: %v", err)
	}

	log.Print("Done!")
}
