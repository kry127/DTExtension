package sink

import (
	"context"
	"kry127.ru/dtextension/go/pkg/api/v0_2"
	"kry127.ru/dtextension/go/pkg/api/v0_2/sink"
)

type mongoSinkService struct {
	*sink.UnimplementedSinkServiceServer
}

func (m *mongoSinkService) Spec(ctx context.Context, req *v0_2.SpecReq) (*v0_2.SpecRsp, error) {
	panic("implement me")
}

func (m *mongoSinkService) Check(ctx context.Context, req *v0_2.CheckReq) (*v0_2.CheckRsp, error) {
	panic("implement me")
}

func (m *mongoSinkService) Write(server sink.SinkService_WriteServer) error {
	panic("implement me")
}

func NewMongoSinkService() *mongoSinkService {
	return &mongoSinkService{}
}