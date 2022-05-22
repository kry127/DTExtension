package sink

import (
	"context"
	common "kry127.ru/dtextension/examples/go_mongo_v0_2/pkg"
	"kry127.ru/dtextension/go/pkg/api/v0_2"
	"kry127.ru/dtextension/go/pkg/api/v0_2/sink"
)

type mongoSinkService struct {
	*sink.UnimplementedSinkServiceServer
}

func (m *mongoSinkService) Spec(ctx context.Context, req *v0_2.SpecReq) (*v0_2.SpecRsp, error) {
	return &v0_2.SpecRsp{
		Result:   common.OkResult(),
		JsonSpec: Specification,
	}, nil
}

func (m *mongoSinkService) Check(ctx context.Context, req *v0_2.CheckReq) (*v0_2.CheckRsp, error) {
	err := Validate(req.GetJsonSettings())
	if err != nil {
		return &v0_2.CheckRsp{
			Result: common.ErrorResult(err.Error()),
		}, nil
	}
	return &v0_2.CheckRsp{
		Result:   common.OkResult(),
	}, nil
}

func (m *mongoSinkService) Write(server sink.SinkService_WriteServer) error {
	panic("implement me")
}

func NewMongoSinkService() *mongoSinkService {
	return &mongoSinkService{}
}