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
	metrics *models.GRPCMetrics
}

func Register(grpcServer grpc.ServiceRegistrar, trackerSvc *tracker.Service, metrics *models.GRPCMetrics) {
	trackingpb.RegisterDriverTrackerServer(grpcServer, &Server{tracker: trackerSvc, metrics: metrics})
}

func (s *Server) PublishLocation(ctx context.Context, req *trackingpb.DriverLocation) (*trackingpb.PublishAck, error) {
	if req.GetId() == "" {
		s.metrics.ProcessingErrors.Add(1)
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	if req.Lat == nil || req.Lng == nil {
		s.metrics.ProcessingErrors.Add(1)
		return nil, status.Error(codes.InvalidArgument, "latitude and longitude are required")
	}

	s.metrics.MessagesReceived.Add(1)

	cmd := models.Command{
		Id:        req.GetId(),
		Lat:       req.Lat,
		Lng:       req.Lng,
		CompanyId: req.GetCompanyId(),
	}
	if err := s.tracker.ProcessDriverUpdate(cmd); err != nil {
		s.metrics.ProcessingErrors.Add(1)
		return nil, status.Errorf(codes.Internal, "publish location: %v", err)
	}

	s.metrics.MessagesSent.Add(1)
	return &trackingpb.PublishAck{Status: "location updated"}, nil
}

func (s *Server) PublishLocationStream(stream trackingpb.DriverTracker_PublishLocationStreamServer) error {
	s.metrics.CurrentConnections.Add(1)
	s.metrics.TotalConnections.Add(1)
	defer s.metrics.CurrentConnections.Add(-1)

	processed := 0
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			s.metrics.MessagesSent.Add(1)
			return stream.SendAndClose(&trackingpb.PublishAck{
				Status: fmt.Sprintf("processed %d location updates", processed),
			})
		}
		if err != nil {
			s.metrics.ProcessingErrors.Add(1)
			return status.Errorf(codes.Unknown, "receive location update: %v", err)
		}

		s.metrics.MessagesReceived.Add(1)

		if req.GetId() == "" {
			s.metrics.ProcessingErrors.Add(1)
			return status.Error(codes.InvalidArgument, "id is required")
		}

		if req.Lat == nil || req.Lng == nil {
			s.metrics.ProcessingErrors.Add(1)
			return status.Error(codes.InvalidArgument, "latitude and longitude are required")
		}

		cmd := models.Command{
			Id:        req.GetId(),
			Lat:       req.Lat,
			Lng:       req.Lng,
			CompanyId: req.GetCompanyId(),
		}
		if err := s.tracker.ProcessDriverUpdate(cmd); err != nil {
			s.metrics.ProcessingErrors.Add(1)
			return status.Errorf(codes.Internal, "process location update: %v", err)
		}
		processed++
	}
}

func (s *Server) TrackDriver(req *trackingpb.TrackDriverRequest, stream trackingpb.DriverTracker_TrackDriverServer) error {
	s.metrics.CurrentConnections.Add(1)
	s.metrics.TotalConnections.Add(1)
	defer s.metrics.CurrentConnections.Add(-1)
	s.metrics.MessagesReceived.Add(1)

	if req.GetDriverId() == "" {
		s.metrics.ProcessingErrors.Add(1)
		return status.Error(codes.InvalidArgument, "driver_id is required")
	}

	conn := transport.NewGRPCWatcherConnection(stream)
	if err := s.tracker.TrackDriver(conn, req.GetDriverId()); err != nil {
		s.metrics.ProcessingErrors.Add(1)
		return status.Errorf(codes.Internal, "track driver: %v", err)
	}
	defer s.tracker.RemoveConnection(conn.ID())

	<-stream.Context().Done()
	return nil
}

func (s *Server) GetDrivers(ctx context.Context, req *trackingpb.GetDriversRequest) (*trackingpb.DriversResponse, error) {
	page := int(req.GetPage())
	if page < 0 {
		return nil, status.Error(codes.InvalidArgument, "page must be greater than or equal to 0")
	}
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
