package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/xerrors"
	"io"
	common "kry127.ru/dtextension/examples/go_mongo_v0_2/pkg"
	"kry127.ru/dtextension/go/pkg/api/v0_2"
	"kry127.ru/dtextension/go/pkg/api/v0_2/sink"
	"log"
	"os"

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

func splitKeysAndNonkeys(table *v0_2.Table) (keys, nonKeys []int) {
	keyColumnIds := []int{}
	nonKeyColumnIds := []int{}
	for i, metaCol := range table.Schema.Columns {
		if metaCol.Key {
			keyColumnIds = append(keyColumnIds, i)
		} else {
			nonKeyColumnIds = append(nonKeyColumnIds, i)
		}
	}
	return keyColumnIds, nonKeyColumnIds
}

func extractColumnValue(column *v0_2.Column, value *v0_2.ColumnValue) bson.E {
	makeJsonOrLeaveOriginal := func(object string) interface{} {
		var arbitraryObject interface{}
		err := json.Unmarshal([]byte(object), &arbitraryObject)
		if err != nil {
			return value.GetJson()
		}
		return arbitraryObject
	}
	switch column.Type {
	case v0_2.ColumnType_COLUMN_TYPE_BOOL:
		return bson.E{Key: column.Name, Value: value.GetBool()}
	case v0_2.ColumnType_COLUMN_TYPE_INT32:
		return bson.E{Key: column.Name, Value: value.GetInt32()}
	case v0_2.ColumnType_COLUMN_TYPE_INT64:
		return bson.E{Key: column.Name, Value: value.GetInt64()}
	case v0_2.ColumnType_COLUMN_TYPE_UINT32:
		return bson.E{Key: column.Name, Value: value.GetUint32()}
	case v0_2.ColumnType_COLUMN_TYPE_UINT64:
		return bson.E{Key: column.Name, Value: value.GetUint64()}
	case v0_2.ColumnType_COLUMN_TYPE_FLOAT:
		return bson.E{Key: column.Name, Value: value.GetFloat()}
	case v0_2.ColumnType_COLUMN_TYPE_DOUBLE:
		return bson.E{Key: column.Name, Value: value.GetDouble()}
	case v0_2.ColumnType_COLUMN_TYPE_JSON:
		return bson.E{Key: column.Name, Value: makeJsonOrLeaveOriginal(value.GetJson())}
	case v0_2.ColumnType_COLUMN_TYPE_DECIMAL:
		return bson.E{Key: column.Name, Value: makeJsonOrLeaveOriginal(value.GetDecimal().GetAsString())}
	case v0_2.ColumnType_COLUMN_TYPE_BIG_DECIMAL:
		return bson.E{Key: column.Name, Value: makeJsonOrLeaveOriginal(value.GetBigDecimal())}
	case v0_2.ColumnType_COLUMN_TYPE_BIG_INTEGER:
		return bson.E{Key: column.Name, Value: makeJsonOrLeaveOriginal(value.GetBigInteger())}
	case v0_2.ColumnType_COLUMN_TYPE_UNIX_TIME:
		return bson.E{Key: column.Name, Value: value.GetUnixTime()}
	case v0_2.ColumnType_COLUMN_TYPE_STRING:
		return bson.E{Key: column.Name, Value: value.GetString_()}
	case v0_2.ColumnType_COLUMN_TYPE_BINARY:
		return bson.E{Key: column.Name, Value: value.GetBinary()}
	default:
		return bson.E{Key: column.Name, Value: nil}
	}
}

func convertToBson(ids []int, columns []*v0_2.Column, values []*v0_2.ColumnValue) bson.D {
	result := bson.D{}
	for _, id := range ids {
		entity := extractColumnValue(columns[id], values[id])
		result = append(result, entity)
	}
	return result
}

func makeInsert(ctx context.Context, collection *mongo.Collection, table *v0_2.Table, changeItem *v0_2.PlainRow) error {
	keyColumnIds, nonKeyColumnIds := splitKeysAndNonkeys(table)
	valueBson := convertToBson(nonKeyColumnIds, table.Schema.Columns, changeItem.Values)
	insertingDocument := bson.D{}
	if len(keyColumnIds) > 0 {
		keyBson := convertToBson(keyColumnIds, table.Schema.Columns, changeItem.Values)
		insertingDocument = append(insertingDocument, bson.E{Key: "_id", Value: keyBson})
	}
	insertingDocument = append(insertingDocument, valueBson...)

	opts := options.InsertOne()
	_, err := collection.InsertOne(ctx, insertingDocument, opts)
	if err != nil {
		return xerrors.Errorf("failed to insert document: %w", err)
	}
	return nil
}

func makeUpsert(ctx context.Context, collection *mongo.Collection, table *v0_2.Table, changeItem *v0_2.PlainRow) error {
	keyColumnIds, nonKeyColumnIds := splitKeysAndNonkeys(table)
	if len(keyColumnIds) == 0 {
		return xerrors.Errorf("no way to identify document without primary keys specified")
	}
	keyBson := convertToBson(keyColumnIds, table.Schema.Columns, changeItem.Values)
	valueBson := convertToBson(nonKeyColumnIds, table.Schema.Columns, changeItem.Values)

	opts := options.Update().SetUpsert(true)
	filter := bson.D{{"_id", keyBson}}
	update := bson.D{{"$set", valueBson}}

	_, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return xerrors.Errorf("failed to upsert document: %w", err)
	}
	return nil
}

func makeDelete(ctx context.Context, collection *mongo.Collection, table *v0_2.Table, changeItem *v0_2.PlainRow) error {
	keyColumnIds, _ := splitKeysAndNonkeys(table)
	keyBson := convertToBson(keyColumnIds, table.Schema.Columns, changeItem.Values)
	if len(keyColumnIds) == 0 {
		return xerrors.Errorf("no way to identify document without primary keys specified")
	}

	opts := options.Delete()
	filter := bson.D{{"_id", keyBson}}

	_, err := collection.DeleteOne(ctx, filter, opts)
	if err != nil {
		return xerrors.Errorf("failed to delete document: %w", err)
	}
	return nil
}

func processChangeItem(ctx context.Context, client *mongo.Client, changeItem *v0_2.DataChangeItem) error {
	table := changeItem.Table
	db := client.Database(table.GetNamespace().GetNamespace())
	collection := db.Collection(table.GetName())
	opType := changeItem.OpType

	switch changeItem := changeItem.Format.(type) {
	case *v0_2.DataChangeItem_PlainRow:
		switch opType {
		case v0_2.OpType_OP_TYPE_INSERT:
			err := makeInsert(ctx, collection, table, changeItem.PlainRow)
			if err != nil {
				return xerrors.Errorf("cannot insert document: %w", err)
			}
		case v0_2.OpType_OP_TYPE_UPDATE:
			err := makeUpsert(ctx, collection, table, changeItem.PlainRow)
			if err != nil {
				return xerrors.Errorf("cannot insert document: %w", err)
			}
		case v0_2.OpType_OP_TYPE_DELETE:
			err := makeDelete(ctx, collection, table, changeItem.PlainRow)
			if err != nil {
				return xerrors.Errorf("cannot insert document: %w", err)
			}
		default:
			return xerrors.Errorf("invalid optype for plain row data change item: %s", opType.String())
		}
	case *v0_2.DataChangeItem_Parquet:
		return xerrors.New("parquet change item format is not implemented yet")
	default:
		return xerrors.Errorf("unknown format type: '%T'", changeItem)
	}
	return nil
}

func (m *mongoSinkService) Write(server sink.SinkService_WriteServer) (outErr error) {
	// catch all panics and make them as error return of this function (no panic policy)
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case error:
				outErr = x
			default:
				outErr = xerrors.Errorf("panic of type '%T': %v", r, r)
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

	var initClient *mongo.Client

	defer func(ctx context.Context, clientRef **mongo.Client) {
		if clientRef == nil {
			return
		}
		client := *clientRef
		if client != nil {
			err := client.Disconnect(ctx)
			if err != nil {
				log.Printf("Error when close mongo client: %v", err)
			}
		}
	}(server.Context(), &initClient)

	for {
		in, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return xerrors.Errorf("cannot receive WriteReq: %w", err)
		}

		writeReq := in.GetControlItemReq()
		if writeReq == nil {
			sendError(xerrors.Errorf("cannot send WriteRsp: %w", err))
			continue
		}
		switch req := writeReq.ControlItemReq.(type) {
		case *sink.WriteControlItemReq_InitReq:
			initReq := req.InitReq

			err := Validate(initReq.GetJsonSettings())
			if err != nil {
				sendError(xerrors.Errorf("validation error: %w", err))
				continue
			}
			params, err := Parse(initReq.JsonSettings)
			if err != nil {
				sendError(xerrors.Errorf("json parameters parse error: %w", err))
				continue
			}

			connString := params.MongoConnectionString
			if params.TLSCertificate != "" {
				f, err := os.CreateTemp("", "DTExtension_")
				if err != nil {
					sendError(xerrors.Errorf("cannot build connection string: %w", err))
					continue
				}
				from := 0
				n := 0
				for from < len(params.TLSCertificate) {
					n, err = f.WriteString(params.TLSCertificate[from:])
					if err != nil {
						break
					}
					from += n
				}
				if err != nil {
					sendError(xerrors.Errorf("cannot build connection string: %w", err))
					continue
				}
				if err := f.Sync(); err != nil {
					sendError(xerrors.Errorf("cannot build connection string: %w", err))
					continue
				}
				if err := f.Close(); err != nil {
					sendError(xerrors.Errorf("cannot build connection string: %w", err))
					continue
				}
				connString = fmt.Sprintf("%s&tls=true&tlscafile=%s", connString, f.Name())
			}
			initClient, err = mongo.NewClient(options.Client().ApplyURI(params.MongoConnectionString))
			if err != nil {
				sendError(xerrors.Errorf("client initialization error: %w", err))
				continue
			}

			err = initClient.Connect(server.Context())
			if err != nil {
				sendError(xerrors.Errorf("client connection error: %w", err))
				continue
			}

			err = server.Send(&sink.WriteRsp{
				Result: common.OkResult(),
				ControlItemRsp: &sink.WriteControlItemRsp{
					ControlItemRsp: &sink.WriteControlItemRsp_InitRsp{
						InitRsp: &v0_2.InitRsp{},
					},
				},
			})
			if err != nil {
				return xerrors.Errorf("cannot send write response: %w", err)
			}

		case *sink.WriteControlItemReq_BeginSnapshotReq:
			beginSnapshotReq := req.BeginSnapshotReq
			table := beginSnapshotReq.Table
			log.Printf("snapshot start for table: %v", table)
			// do nothing right now
			err := server.Send(&sink.WriteRsp{
				Result: common.OkResult(),
				ControlItemRsp: &sink.WriteControlItemRsp{
					ControlItemRsp: &sink.WriteControlItemRsp_BeginSnapshotRsp{
						BeginSnapshotRsp: &sink.WriteBeginSnapshotRsp{
							SnapshotState: []byte{},
						},
					},
				},
			})
			if err != nil {
				return xerrors.Errorf("cannot send write response: %w", err)
			}

		case *sink.WriteControlItemReq_DoneSnapshotReq:
			doneSnapshotReq := req.DoneSnapshotReq
			table := doneSnapshotReq.Table
			log.Printf("snapshot done for table: %v", table)
			// do nothing right now
			err := server.Send(&sink.WriteRsp{
				Result: common.OkResult(),
				ControlItemRsp: &sink.WriteControlItemRsp{
					ControlItemRsp: &sink.WriteControlItemRsp_DoneSnapshotRsp{
						DoneSnapshotRsp: &sink.WriteDoneSnapshotRsp{},
					},
				},
			})
			if err != nil {
				return xerrors.Errorf("cannot send write response: %w", err)
			}
		case *sink.WriteControlItemReq_ItemReq:
			itemReq := req.ItemReq
			if initClient == nil {
				sendError(xerrors.Errorf("send InitReq with connection parameters first"))
				continue
			}

			switch itemReq := itemReq.WriteItemReq.(type) {
			case *sink.WriteItemReq_ChangeItem:
				changeItem := itemReq.ChangeItem
				switch changeItem := changeItem.ChangeItem.(type) {
				case *v0_2.ChangeItem_HomoChangeItem:
					sendError(xerrors.Errorf("processing of homo change items is not supported yet"))
					continue
				case *v0_2.ChangeItem_DataChangeItem:
					dataChangeItem := changeItem.DataChangeItem
					err := processChangeItem(server.Context(), initClient, dataChangeItem)
					if err != nil {
						sendError(xerrors.Errorf("cannot process change item: %w", err))
						continue
					}
				default:
					sendError(xerrors.Errorf("unknown change item type '%T'", changeItem))
					continue
				}
			case *sink.WriteItemReq_CheckPoint_:
				err := server.Send(&sink.WriteRsp{
					Result: common.OkResult(),
					ControlItemRsp: &sink.WriteControlItemRsp{
						ControlItemRsp: &sink.WriteControlItemRsp_ItemRsp{
							ItemRsp: &sink.WriteItemRsp{},
						},
					},
				})
				if err != nil {
					return xerrors.Errorf("cannot send write response: %w", err)
				}
			default:
				sendError(xerrors.Errorf("unknown item request type '%T'", itemReq))
			}
		default:
			sendError(xerrors.Errorf("unknown request type '%T'", req))
		}
	}
}

func NewMongoSinkService() *mongoSinkService {
	return &mongoSinkService{}
}
