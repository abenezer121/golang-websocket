package grpcapi

import (
	"context"
	"errors"
	"fastsocket/grpc/trackingpb"
	"fastsocket/models"
	"fastsocket/tracker"
	"fastsocket/transport"
	"fmt"
	"io"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	trackingpb.UnimplementedDriverTrackerServer

	tracker *tracker.Service
}

func Register(grpcServer grpc.ServiceRegistrar, trackerSvc *tracker.Service) {
	trackingpb.RegisterDriverTrackerServer(grpcServer, &Server{tracker: trackerSvc})
}

func (s *Server) PublishLocation(ctx context.Context, req *trackingpb.DriverLocation) (*trackingpb.PublishAck, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	cmd := models.Command{
		Id:        req.GetId(),
		Lat:       float64Ptr(req.GetLat()),
		Lng:       float64Ptr(req.GetLng()),
		CompanyId: req.GetCompanyId(),
	}
	if err := s.tracker.ProcessDriverUpdate(cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "publish location: %v", err)
	}

	return &trackingpb.PublishAck{Status: "location updated"}, nil
}

func (s *Server) PublishLocationStream(stream trackingpb.DriverTracker_PublishLocationStreamServer) error {
	processed := 0
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&trackingpb.PublishAck{
				Status: fmt.Sprintf("processed %d location updates", processed),
			})
		}
		if err != nil {
			return status.Errorf(codes.Unknown, "receive location update: %v", err)
		}
		if req.GetId() == "" {
			return status.Error(codes.InvalidArgument, "id is required")
		}

		cmd := models.Command{
			Id:        req.GetId(),
			Lat:       float64Ptr(req.GetLat()),
			Lng:       float64Ptr(req.GetLng()),
			CompanyId: req.GetCompanyId(),
		}
		if err := s.tracker.ProcessDriverUpdate(cmd); err != nil {
			return status.Errorf(codes.Internal, "process location update: %v", err)
		}
		processed++
	}
}

func (s *Server) TrackDriver(req *trackingpb.TrackDriverRequest, stream trackingpb.DriverTracker_TrackDriverServer) error {
	if req.GetDriverId() == "" {
		return status.Error(codes.InvalidArgument, "driver_id is required")
	}

	conn := transport.NewGRPCWatcherConnection(stream)
	if err := s.tracker.TrackDriver(conn, req.GetDriverId()); err != nil {
		return status.Errorf(codes.Internal, "track driver: %v", err)
	}
	defer s.tracker.RemoveConnection(conn.ID())

	<-stream.Context().Done()
	return nil
}

func (s *Server) GetDrivers(ctx context.Context, req *trackingpb.GetDriversRequest) (*trackingpb.DriversResponse, error) {
	page := int(req.GetPage())
	if page == 0 {
		page = 1
	}

	drivers, total, err := s.tracker.GetDrivers(page, 100)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get drivers: %v", err)
	}

	return &trackingpb.DriversResponse{
		Command: "get-drivers",
		Drivers: transport.DriversToProto(drivers),
		Total:   int32(total),
	}, nil
}

func (s *Server) GetBBox(ctx context.Context, req *trackingpb.GetBBoxRequest) (*trackingpb.DriversResponse, error) {
	if err := transport.ValidateBBox(req.GetMinLat(), req.GetMinLng(), req.GetMaxLat(), req.GetMaxLng()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	drivers, err := s.tracker.GetDriversInBBox(req.GetMinLat(), req.GetMinLng(), req.GetMaxLat(), req.GetMaxLng())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get bbox: %v", err)
	}

	return &trackingpb.DriversResponse{
		Command: "get-bbox",
		Drivers: transport.DriversToProto(drivers),
		Total:   int32(len(drivers)),
	}, nil
}

func float64Ptr(value float64) *float64 {
	return &value
}
