package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/airbusgeo/geocube-client-go/pb"
)

type AOI [][][][2]float64

type Record struct {
	ID    string
	Name  string
	Time  time.Time
	Tags  map[string]string
	AOIID string
	AOI   *AOI
}

func recordFromPb(pbrec *pb.Record) *Record {
	if pbrec == nil {
		return nil
	}
	return &Record{
		ID:    pbrec.Id,
		Name:  pbrec.Name,
		Time:  pbrec.Time.AsTime(),
		Tags:  pbrec.Tags,
		AOIID: pbrec.AoiId,
		AOI:   aoiFromPb(pbrec.Aoi),
	}
}

// ToString returns a string representation of a Record
func (r *Record) ToString() string {
	s := fmt.Sprintf("Record %s:\n"+
		"  Id:              %s\n"+
		"  Time:            %s\n"+
		"  AOI ID:          %s\n"+
		"  Tags:            ", r.Name, r.ID, r.Time, r.AOIID)
	appendDict(r.Tags, &s)
	if r.AOI != nil {
		g, _ := GeometryFromAOI(*r.AOI).MarshalJSON()
		s += fmt.Sprintf("  AOI:             %s\n", g)
	}

	return s
}

func (r *Record) ToPb() pb.Record {
	t := timestamppb.New(r.Time)
	return pb.Record{
		Id:    r.ID,
		Name:  r.Name,
		Time:  t,
		Tags:  map[string]string{},
		AoiId: r.AOIID,
		Aoi:   &pb.AOI{},
	}
}

// CreateAOI creates an aoi from a geometry
// If error EntityAlreadyExists, returns the id of the existing one and the error.
func (c Client) CreateAOI(ctx context.Context, g AOI) (string, error) {
	resp, err := c.gcc.CreateAOI(ctx, &pb.CreateAOIRequest{Aoi: pbFromAOI(g)})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			for _, detail := range s.Details() {
				switch t := detail.(type) {
				case *errdetails.ResourceInfo:
					return t.GetResourceName(), grpcError(err)
				}
			}
		}
		return "", grpcError(err)
	}
	return resp.GetId(), nil
}

// GetAOI retrieves an AOI from a aoi_id
func (c Client) GetAOI(ctx context.Context, aoiID string) (AOI, error) {
	resp, err := c.gcc.GetAOI(ctx, &pb.GetAOIRequest{Id: aoiID})
	if err != nil {
		return AOI{}, grpcError(err)
	}
	return *aoiFromPb(resp.Aoi), nil
}

// CreateRecord creates a record with an aoi, a name and a date
func (c Client) CreateRecord(ctx context.Context, name, aoiID string, date time.Time, tags map[string]string) ([]string, error) {
	ts := timestamppb.New(date)
	if err := ts.CheckValid(); err != nil {
		return nil, err
	}
	record := &pb.NewRecord{
		Name:  name,
		Time:  ts,
		Tags:  tags,
		AoiId: aoiID}

	resp, err := c.gcc.CreateRecords(ctx, &pb.CreateRecordsRequest{Records: []*pb.NewRecord{record}})
	if err != nil {
		return nil, grpcError(err)
	}
	return resp.GetIds(), nil
}

// CreateRecords creates a batch of records with aois, names and dates
func (c Client) CreateRecords(ctx context.Context, names, aoiIDs []string, dates []time.Time, tags []map[string]string) ([]string, error) {
	if len(names) != len(aoiIDs) || len(names) != len(dates) || len(names) != len(tags) {
		return nil, fmt.Errorf("CreateRecords: all parameters must be the same length")
	}

	records := make([]*pb.NewRecord, len(dates))
	for i, t := range dates {
		ts := timestamppb.New(t)
		if err := ts.CheckValid(); err != nil {
			return nil, err
		}
		records[i] = &pb.NewRecord{
			Name:  names[i],
			Time:  ts,
			Tags:  tags[i],
			AoiId: aoiIDs[i]}
	}

	resp, err := c.gcc.CreateRecords(ctx, &pb.CreateRecordsRequest{Records: records})
	if err != nil {
		return nil, grpcError(err)
	}
	return resp.GetIds(), nil
}

// DeleteRecords deletes a batch of records iif no dataset has reference on
// Returns the number of deleted records
func (c Client) DeleteRecords(ctx context.Context, ids []string) (int64, error) {
	resp, err := c.gcc.DeleteRecords(ctx, &pb.DeleteRecordsRequest{Ids: ids})
	if err != nil {
		return 0, grpcError(err)
	}
	return resp.Nb, nil
}

// AddRecordsTags add or update existing tag form list of records
// returns the number of updated records
func (c Client) AddRecordsTags(ctx context.Context, ids []string, tags map[string]string) (int64, error) {
	resp, err := c.gcc.AddRecordsTags(ctx, &pb.AddRecordsTagsRequest{
		Ids:  ids,
		Tags: tags,
	})
	if err != nil {
		return 0, grpcError(err)
	}
	return resp.Nb, nil
}

// AddRecordsTags delete tags from list of records
// returns the number of updated records
func (c Client) RemoveRecordsTags(ctx context.Context, ids []string, tags []string) (int64, error) {
	resp, err := c.gcc.RemoveRecordsTags(ctx, &pb.RemoveRecordsTagsRequest{
		Ids:     ids,
		TagsKey: tags,
	})
	if err != nil {
		return 0, grpcError(err)
	}
	return resp.Nb, nil
}

func (c Client) streamListRecords(ctx context.Context, name string, tags map[string]string, aoi AOI, fromTime, toTime time.Time, limit, page int, returnAOI bool) (pb.Geocube_ListRecordsClient, error) {

	fromTs := timestamppb.New(fromTime)
	if err := fromTs.CheckValid(); err != nil {
		return nil, err
	}
	toTs := timestamppb.New(toTime)
	if err := toTs.CheckValid(); err != nil {
		return nil, err
	}

	res, err := c.gcc.ListRecords(ctx, &pb.ListRecordsRequest{
		Name:     name,
		Tags:     tags,
		Aoi:      pbFromAOI(aoi),
		FromTime: fromTs,
		ToTime:   toTs,
		Limit:    int32(limit),
		Page:     int32(page),
		WithAoi:  returnAOI,
	})
	return res, grpcError(err)
}

// ListRecords lists records that fit the search parameters (all are optionnal)
func (c Client) ListRecords(ctx context.Context, name string, tags map[string]string, aoi AOI, fromTime, toTime time.Time, limit, page int, returnAOI bool) ([]*Record, error) {
	streamrecords, err := c.streamListRecords(ctx, name, tags, aoi, fromTime, toTime, limit, page, returnAOI)
	if err != nil {
		return nil, err
	}
	records := []*Record{}

	for {
		resp, err := streamrecords.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, recordFromPb(resp.Record))
	}

	return records, nil
}

// convert types take an int and return a string value.
type GroupByKeyFunc func(*Record) string

func GroupByDayKey(record *Record) string {
	return record.Time.Format("2006-01-02")
}

// GroupBy groups the records of the list by the key provided by the func_key(Record)
// Returns a list of grouped records ID
func GroupBy(records []*Record, funcKey GroupByKeyFunc) [][]string {
	results := [][]string{}
	indices := map[string]int{}
	for _, r := range records {
		key := funcKey(r)
		index, ok := indices[key]
		if !ok {
			index = len(results)
			indices[key] = index
			results = append(results, []string{})
		}
		results[index] = append(results[index], r.ID)
	}
	return results
}
