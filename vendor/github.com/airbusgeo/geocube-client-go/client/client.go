package client

import (
	"context"

	pb "github.com/airbusgeo/geocube-client-go/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Client struct {
	gcc pb.GeocubeClient
	ctx context.Context
}

// New creates a new client
func New(ctx context.Context, grpconn *grpc.ClientConn) Client {
	return Client{
		gcc: pb.NewGeocubeClient(grpconn),
		ctx: ctx,
	}
}

// Dial creates a new client connected to the server
func Dial(ctx context.Context, server string, creds credentials.TransportCredentials, apikey string) (Client, error) {
	opts := []grpc.DialOption{}
	if creds == nil {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}
	if apikey != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(TokenAuth{Token: apikey}))
	}

	grpcconn, err := grpc.Dial(server, opts...)
	if err != nil {
		return Client{}, grpcError(err)
	}
	return New(ctx, grpcconn), nil
}

// ServerVersion returns the version of the Geocube serveur
func (c Client) ServerVersion(ctx context.Context) (string, error) {
	resp, err := c.gcc.Version(ctx, &pb.GetVersionRequest{})
	if err != nil {
		return "", grpcError(err)
	}
	return resp.Version, nil
}
