package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/airbusgeo/geocube-client-go/pb"
)

type Job pb.Job
type ConsolidationParams pb.ConsolidationParams

// IndexDataset indexes a dataset
func (c Client) IndexDataset(ctx context.Context, uri string, managed bool, containerSubdir, recordID, instanceID string, bands []int64, dformat *DataFormat, realMin, realMax, exponent float64) error {
	dataset := &pb.Dataset{
		RecordId:        recordID,
		InstanceId:      instanceID,
		ContainerSubdir: containerSubdir,
		Bands:           bands,
		Dformat:         (*pb.DataFormat)(dformat),
		RealMinValue:    realMin,
		RealMaxValue:    realMax,
		Exponent:        exponent,
	}
	return c.IndexDatasets(ctx, uri, managed, []*pb.Dataset{dataset})
}

// IndexDatasets indexes a batch of datasets
func (c Client) IndexDatasets(ctx context.Context, uri string, managed bool, datasets []*pb.Dataset) error {
	_, err := c.gcc.IndexDatasets(ctx,
		&pb.IndexDatasetsRequest{Container: &pb.Container{
			Uri:      uri,
			Managed:  managed,
			Datasets: datasets}})

	return grpcError(err)
}

// CleanJobs removes the terminated jobs
// nameLike, state : [optional] filter by name or state (DONE/FAILED)
func (c Client) CleanJobs(ctx context.Context, nameLike, state string) (int32, error) {
	cresp, err := c.gcc.CleanJobs(ctx, &pb.CleanJobsRequest{
		NameLike: nameLike,
		State:    state,
	})
	if err != nil {
		return 0, grpcError(err)
	}
	return cresp.GetCount(), nil
}

// ConfigConsolidation configures the parameters associated to this variable
func (c Client) ConfigConsolidation(ctx context.Context, variableID string, dformat *DataFormat, exponent float64, compression int, resamplingAlg string, storageClass int) error {
	_, err := c.gcc.ConfigConsolidation(ctx, &pb.ConfigConsolidationRequest{
		VariableId: variableID,
		ConsolidationParams: &pb.ConsolidationParams{
			Dformat:       (*pb.DataFormat)(dformat),
			Exponent:      exponent,
			Compression:   pb.ConsolidationParams_Compression(compression),
			ResamplingAlg: toResampling(resamplingAlg),
			StorageClass:  pb.StorageClass(storageClass),
		}})
	return grpcError(err)
}

// GetConsolidationParams read the consolidation parameters associated to this variable
func (c Client) GetConsolidationParams(ctx context.Context, variableID string) (*ConsolidationParams, error) {
	resp, err := c.gcc.GetConsolidationParams(ctx, &pb.GetConsolidationParamsRequest{VariableId: variableID})
	if Code(err) == codes.NotFound {
		return nil, nil
	}
	return (*ConsolidationParams)(resp.ConsolidationParams), grpcError(err)
}

// ConsolidateDatasetsFromRecords starts a consolidation job of the datasets defined by the given parameters
func (c Client) ConsolidateDatasetsFromRecords(ctx context.Context, name string, instanceID, layoutName string, recordsID []string) (string, error) {
	resp, err := c.gcc.Consolidate(ctx,
		&pb.ConsolidateRequest{
			JobName:       name,
			LayoutName:    layoutName,
			InstanceId:    instanceID,
			RecordsLister: &pb.ConsolidateRequest_Records{Records: &pb.RecordIdList{Ids: recordsID}},
		})

	if err != nil {
		return "", grpcError(err)
	}

	return resp.GetJobId(), nil
}

// ConsolidateDatasetsFromFilters starts a consolidation job of the datasets defined by the given parameters
func (c Client) ConsolidateDatasetsFromFilters(ctx context.Context, name string, instanceID, layoutName string, tags map[string]string, fromTime, toTime time.Time) (string, error) {
	fromTs := timestamppb.New(fromTime)
	if err := fromTs.CheckValid(); err != nil {
		return "", err
	}
	toTs := timestamppb.New(toTime)
	if err := toTs.CheckValid(); err != nil {
		return "", err
	}
	resp, err := c.gcc.Consolidate(ctx,
		&pb.ConsolidateRequest{
			JobName:       name,
			LayoutName:    layoutName,
			InstanceId:    instanceID,
			RecordsLister: &pb.ConsolidateRequest_Filters{Filters: &pb.RecordFilters{Tags: tags, FromTime: fromTs, ToTime: toTs}},
		})

	if err != nil {
		return "", grpcError(err)
	}

	return resp.GetJobId(), nil
}

// ToString returns a string with a representation of the job
func (j *Job) ToString() string {
	return fmt.Sprintf("Job %s:\n"+
		"  Id:              %s\n"+
		"  Type:            %s\n"+
		"  State:           %s\n"+
		"  Active tasks:    %d\n"+
		"  Failed tasks:    %d\n"+
		"  Creation:        %s\n"+
		"  LastUpdate:      %s\n"+
		"  Logs:            \n - %s",
		j.Name, j.Id, j.Type, j.State, j.ActiveTasks, j.FailedTasks, j.CreationTime.AsTime().Format("2 Jan 2006 15:04:05"),
		j.LastUpdateTime.AsTime().Format("2 Jan 2006 15:04:05"), strings.Join(j.Logs, "\n - "))
}

// ListJobs returns the jobs with a name like name (or all if name="")
func (c Client) ListJobs(ctx context.Context, nameLike string) ([]*Job, error) {
	jresp, err := c.gcc.ListJobs(ctx, &pb.ListJobsRequest{NameLike: nameLike})
	if err != nil {
		return nil, grpcError(err)
	}
	var jobs []*Job
	for _, j := range jresp.Jobs {
		jobs = append(jobs, (*Job)(j))
	}
	return jobs, nil
}

// GetJob returns the job with the given ID
func (c Client) GetJob(ctx context.Context, jobID string) (*Job, error) {
	jresp, err := c.gcc.GetJob(ctx, &pb.GetJobRequest{Id: jobID})
	if err != nil {
		return nil, grpcError(err)
	}
	return (*Job)(jresp.GetJob()), nil
}

// RetryJob retries the job with the given ID
func (c Client) RetryJob(ctx context.Context, jobID string, forceAnyState bool) error {
	_, err := c.gcc.RetryJob(ctx, &pb.RetryJobRequest{Id: jobID, ForceAnyState: forceAnyState})
	return grpcError(err)
}

// CancelJob retries the job with the given ID
func (c Client) CancelJob(ctx context.Context, jobID string) error {
	_, err := c.gcc.CancelJob(ctx, &pb.CancelJobRequest{Id: jobID})
	return grpcError(err)
}
