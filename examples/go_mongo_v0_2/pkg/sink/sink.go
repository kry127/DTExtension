package sink

import (
	"context"
	"fmt"
	"io"
	common "kry127.ru/dtextension/examples/go_mongo_v0_2/pkg"
	"kry127.ru/dtextension/go/pkg/api/v0_2"
	"kry127.ru/dtextension/go/pkg/api/v0_2/sink"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
		Result: common.OkResult(),
	}, nil
}

func (m *mongoSinkService) Write(server sink.SinkService_WriteServer) (err error) {
	// catch all panics and make them as error return of this function (no panic policy)
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case error:
				err = x
			default:
				err = fmt.Errorf("panic of type '%T': %v", r, r)
			}
		}
	}()

	sendError := func(error error) {
		err := server.Send(&sink.WriteRsp{
			Result:         common.ErrorResult(error.Error()),
			ControlItemRsp: nil,
		})
		if err != nil {
			panic(err)
		}
	}

	var client *mongo.Client

	defer func(ctx context.Context, clientRef **mongo.Client) {
		if clientRef == nil {
			return
		}
		client = *clientRef
		if client != nil {
			err := client.Disconnect(ctx)
			if err != nil {
				log.Printf("Error when close mongo client: %v", err)
			}
		}
	}(server.Context(), &client)

	for {
		in, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("cannot receive WriteReq: %w", err)
		}

		writeReq := in.GetControlItemReq()
		if writeReq == nil {
			sendError(fmt.Errorf("cannot send WriteRsp: %w", err))
		}
		switch req := writeReq.ControlItemReq.(type) {
		case *sink.WriteControlItemReq_InitReq:
			initReq := req.InitReq

			err := Validate(initReq.GetJsonSettings())
			if err != nil {
				sendError(fmt.Errorf("validation error: %w", err))
			}
			params, err := Parse(initReq.JsonSettings)
			if err != nil {
				sendError(fmt.Errorf("json parameters parse error: %w", err))
			}

			client, err = mongo.NewClient(options.Client().ApplyURI(params.MongoConnectionString))
			if err != nil {
				sendError(fmt.Errorf("client initialization error: %w", err))
			}

			err = client.Connect(server.Context())
			if err != nil {
				sendError(fmt.Errorf("client connection error: %w", err))
			}

		case *sink.WriteControlItemReq_BeginSnapshotReq:
			beginSnapshotReq := req.BeginSnapshotReq
		case *sink.WriteControlItemReq_DoneSnapshotReq:
			doneSnapshotReq := req.DoneSnapshotReq
		case *sink.WriteControlItemReq_ItemReq:
			itemReq := req.ItemReq
		default:
			sendError(fmt.Errorf("unknown request type '%T'", req))
		}
	}
}

func NewMongoSinkService() *mongoSinkService {
	return &mongoSinkService{}
}
