package client

import (
	"context"
	"fmt"

	pb "github.com/airbusgeo/geocube-client-go/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type DownloaderClient struct {
	gdcc pb.GeocubeDownloaderClient
}

type Client struct {
	gcc      pb.GeocubeClient
	dlClient *DownloaderClient
}

func (clt *Client) GetDownloaderClient() DownloaderClient {
	return *clt.dlClient
}

type Connector struct {
	Server string
	Creds  credentials.TransportCredentials
	ApiKey string
}

type DownloaderConnector Connector

type ClientConnector struct {
	Connector
	DownloaderConnector *DownloaderConnector
}

// ServerVersion returns the version of the Geocube serveur
func (c *Client) ServerVersion(ctx context.Context) (string, error) {
	resp, err := c.gcc.Version(ctx, &pb.GetVersionRequest{})
	if err != nil {
		return "", grpcError(err)
	}
	return resp.Version, nil
}

// Version returns the version of the Geocube downloader
func (c DownloaderClient) Version(ctx context.Context) (string, error) {
	resp, err := c.gdcc.Version(ctx, &pb.GetVersionRequest{})
	if err != nil {
		return "", grpcError(err)
	}
	return resp.Version, nil
}

func (c Connector) connect() (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{}
	if c.Creds == nil {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts, grpc.WithTransportCredentials(c.Creds))
	}
	if c.ApiKey != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(TokenAuth{Token: c.ApiKey}))
	}
	grpcconn, err := grpc.Dial(c.Server, opts...)
	if err != nil {
		return nil, grpcError(err)
	}
	return grpcconn, nil
}

// Dial creates a new client connected to the server
func (c *ClientConnector) Dial() (Client, error) {
	grpconn, err := c.connect()
	if err != nil {
		return Client{}, fmt.Errorf("Dial: %w", err)
	}
	var dlClient *DownloaderClient
	if c.DownloaderConnector != nil {
		dl, err := c.DownloaderConnector.Dial()
		if err != nil {
			return Client{}, fmt.Errorf("Dial Downloader: %w", err)
		}
		dlClient = &dl
	}
	return Client{
		gcc:      pb.NewGeocubeClient(grpconn),
		dlClient: dlClient,
	}, nil
}

// Dial creates a new downloader client connected to a downloader service
func (dl *DownloaderConnector) Dial() (DownloaderClient, error) {
	grpconn, err := Connector(*dl).connect()
	if err != nil {
		return DownloaderClient{}, err
	}
	return DownloaderClient{
		gdcc: pb.NewGeocubeDownloaderClient(grpconn),
	}, nil
}
