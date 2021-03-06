# proto imports
import traceback

import api.v0_2.source.source_service_pb2_grpc as src_grpc
import api.v0_2.source.source_service_pb2 as src
import api.v0_2.source.read_pb2 as read_ctl
import api.v0_2.common_pb2 as common
import api.v0_2.data_pb2 as data

# common library imports
import bisect
import codecs
import csv
import io
import pathlib
import typing
import uuid
import json
import jsonlines
from jsonschema import validate
import boto3

from source_params import SourceParams, PartitioningType, FileType


def mkOk() -> common.Result:
    return common.Result(ok=True)


def mkErr(error: str) -> common.Result:
    return common.Result(ok=False, error=error)


class CsvSchema:
    def __init__(self, amount):
        self.amount = amount

    def __eq__(self, other):
        if isinstance(other, CsvSchema):
            return False
        return self.amount == other.amount


class JsonLinesSchema:
    def __eq__(self, other):
        if isinstance(other, JsonLinesSchema):
            return False
        return True


# this function returns mapping from key (without prefix) to schemas
def discover_objects(params: SourceParams) -> typing.Dict[str, typing.Union[CsvSchema, JsonLinesSchema]]:
    session = boto3.Session(aws_access_key_id=params.aws_access_key_id,
                            aws_secret_access_key=params.aws_secret_access_key)
    s3 = session.client('s3')

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=params.bucket, Prefix=params.prefix)

    objects = {}
    for page in pages:
        for obj in page['Contents']:
            key = obj['Key'].removeprefix(params.prefix)
            if not params.filter_re.search(key):
                continue
            if params.partitioning_type == PartitioningType.PLAIN_TABLES \
                    and params.plain_tables \
                    and params.plain_tables.depth > 0:
                key_path = pathlib.PurePath(key)
                id = max(len(key_path.parents) - 1 - params.plain_tables.depth, 0)
                key = str(key_path.parents[id])

            if params.file_type == FileType.CSV:
                response = s3.get_object(Bucket=params.bucket, Key=obj['Key'])
                data = io.TextIOWrapper(response['Body'])
                line_width = -1
                for line in data:
                    reader = csv.reader([line],
                                        delimiter=params.csv_delimeter,
                                        quotechar=params.csv_quotechar,
                                        skipinitialspace=True
                                        )
                    for r in reader:
                        line_width = len(r)
                        break
                    break

                obj_schema = objects.get(key)
                if obj_schema and obj_schema != CsvSchema(line_width):
                    raise ValueError("Incompatible schema")
                else:
                    objects[key] = CsvSchema(line_width)
            elif params.file_type == FileType.JSON_LINES:
                obj_schema = objects.get(key)
                if obj_schema and obj_schema != JsonLinesSchema():
                    raise ValueError("Incompatible schema")
                else:
                    objects[key] = JsonLinesSchema()
    return objects


def mkJsonLinesSchema(namespace, name):
    return data.Table(
        namespace=data.Namespace(namespace=namespace),
        name=name,
        schema=data.Schema(
            # TODO separate CSV with and without header
            columns=[data.Column(
                name=f"value",
                key=False,
                type=data.COLUMN_TYPE_JSON
            )]
        )
    )


def mkCsvSchema(namespace, name, width):
    return data.Table(
        namespace=data.Namespace(namespace=namespace),
        name=name,
        schema=data.Schema(
            # TODO separate CSV with and without header
            columns=[data.Column(
                name=f"column{i}",
                key=False,
                type=data.COLUMN_TYPE_STRING)
                for i in range(width)]
        )
    )


# this function converts discovered objects (from 'discover_objects' function) to table proto description
# TODO we can add byte counter to schema and
# snapshot optimization: https://alexwlchan.net/2019/02/working-with-large-s3-objects/
def convert_to_proto(params: SourceParams, objects: typing.Dict[str, typing.Union[CsvSchema, JsonLinesSchema]]):
    tables = []
    for key, schema in objects.items():
        if isinstance(schema, CsvSchema):
            tables.append(mkCsvSchema(params.bucket, key, schema.amount))
        elif isinstance(schema, JsonLinesSchema):
            tables.append(mkJsonLinesSchema(params.bucket, key))
        else:
            raise ValueError(f"Invalid schema: {schema}")
    return tables


# this function returns all objects belonging to presented key
# objects are real keys, not truncated from the left by prefix
def list_s3_keys(params: SourceParams, for_key: str) -> [str]:
    session = boto3.Session(aws_access_key_id=params.aws_access_key_id,
                            aws_secret_access_key=params.aws_secret_access_key)
    s3 = session.client('s3')

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=params.bucket, Prefix=params.prefix)

    result = []
    for page in pages:
        for obj in page['Contents']:
            key = obj['Key'].removeprefix(params.prefix)
            if not params.filter_re.search(key):
                continue
            if params.partitioning_type == PartitioningType.PLAIN_TABLES \
                    and params.plain_tables \
                    and params.plain_tables.depth > 0:
                key_path = pathlib.PurePath(key)
                id = max(len(key_path.parents) - 1 - params.plain_tables.depth, 0)
                key = str(key_path.parents[id])
            if key == for_key:
                result.append(obj['Key'])
    return result


# this generator generates change items from requested key in bucket
def produceChangeItems(params: SourceParams, key: str):
    table_name = key.removeprefix(params.prefix)

    session = boto3.Session(aws_access_key_id=params.aws_access_key_id,
                            aws_secret_access_key=params.aws_secret_access_key)
    s3 = session.client('s3')
    response = s3.get_object(Bucket=params.bucket, Key=key)
    reader = codecs.getreader("utf-8")(response["Body"])
    if params.file_type == FileType.CSV:
        # https://dev.to/shihanng/how-to-read-csv-file-from-amazon-s3-in-python-4ee9
        for row in csv.DictReader(reader,
                                  delimiter=params.csv_delimeter,
                                  quotechar=params.csv_quotechar,
                                  skipinitialspace=True,
                                  ):
            values = [data.ColumnValue(string=value) for _, value in row.items()]
            yield src.ReadRsp(result=mkOk(), read_ctl_rsp=read_ctl.ReadCtlRsp(read_change_rsp=read_ctl.ReadChangeRsp(
                change_item=data.ChangeItem(
                    data_change_item=data.DataChangeItem(
                        op_type=data.OP_TYPE_INSERT,
                        table=mkCsvSchema(params.bucket, table_name, len(values)),
                        plain_row=data.PlainRow(values=values)
                    )
                )
            )))
    elif params.file_type == FileType.JSON_LINES:
        with jsonlines.Reader(reader) as reader:
            for json_line in reader:
                yield src.ReadRsp(result=mkOk(),
                  read_ctl_rsp=read_ctl.ReadCtlRsp(read_change_rsp=read_ctl.ReadChangeRsp(
                      change_item=data.ChangeItem(
                          data_change_item=data.DataChangeItem(
                              op_type=data.OP_TYPE_INSERT,
                              table=mkJsonLinesSchema(params.bucket, table_name),
                              format=data.PlainRow(values=[
                                  data.ColumnValue(json=json_line)
                              ])
                          )
                      )
                  )))

    # after all, yield check point with current file
    data_range = common.DataRange()
    col_value = getattr(data_range, 'from')
    col_value.string = key
    yield src.ReadRsp(result=mkOk(), read_ctl_rsp=read_ctl.ReadCtlRsp(read_change_rsp=read_ctl.ReadChangeRsp(
        checkpoint=read_ctl.ReadChangeRsp.CheckPoint(
            cursor=common.Cursor(
                column_cursor=common.ColumnCursor(
                    column=data.Column(
                        name=f"filename",
                        key=True,
                        type=data.COLUMN_TYPE_STRING
                    ),
                    data_range=data_range
                )
            )
        )
    )))


class S3Source(src_grpc.SourceServiceServicer):
    def Spec(self, request, context):
        try:
            with open('source_spec.json') as f:
                spec = f.read()
            return common.SpecRsp(result=mkOk(), json_spec=spec)
        except Exception as e:
            print(traceback.format_exc())
            return common.SpecRsp(result=mkErr(str(e)))

    def Check(self, request, context):
        try:
            settings = json.loads(request.json_settings)
            with open('source_spec.json') as f:
                json_spec = f.read()
            spec = json.loads(json_spec)
            validate(instance=settings, schema=spec)
            return common.CheckRsp(result=mkOk())
        except Exception as e:
            print(traceback.format_exc())
            return common.SpecRsp(result=mkErr(str(e)))

    def Discover(self, request, context):
        try:
            settings = json.loads(request.json_settings)
            with open('source_spec.json') as f:
                json_spec = f.read()
            spec = json.loads(json_spec)
            validate(instance=settings, schema=spec)
            params = SourceParams(**settings)

            result = discover_objects(params)
            tables = convert_to_proto(params, result)
            return src.DiscoverRsp(result=mkOk(), tables=tables)

        except Exception as e:
            print(traceback.format_exc())
            return src.DiscoverRsp(result=mkErr(str(e)))

    def Read(self, request_iterator, context):
        params: typing.Union[SourceParams, None] = None
        for req in request_iterator:
            table = req.table
            request = req.read_ctl_req
            try:
                req_type = request.WhichOneof('ctl_req')
                if req_type == "init_req":
                    init_req = request.init_req
                    client_id = init_req.client_id
                    if not client_id:
                        client_id = str(uuid.uuid4())

                    json_settings = init_req.json_settings
                    settings = json.loads(json_settings)
                    with open('source_spec.json') as f:
                        json_spec = f.read()
                    spec = json.loads(json_spec)
                    validate(instance=settings, schema=spec)
                    params = SourceParams(**settings)

                    yield src.ReadRsp(result=mkOk(),
                                      read_ctl_rsp=read_ctl.ReadCtlRsp(init_rsp=common.InitRsp(client_id=client_id)))
                elif req_type == "cursor_req":
                    # cursor is iterable by key names in bucket correspoiding to table. Begin with default one.
                    data_range = common.DataRange()
                    col_value = getattr(data_range, 'from')
                    col_value.string = ''
                    yield src.ReadRsp(result=mkOk(), read_ctl_rsp=read_ctl.ReadCtlRsp(cursor_rsp=read_ctl.CursorRsp(
                        cursor=common.Cursor(
                            column_cursor=common.ColumnCursor(
                                column=data.Column(
                                    name=f"filename",
                                    key=True,
                                    type=data.COLUMN_TYPE_STRING
                                ),
                                data_range=data_range
                            )
                        )
                    )))
                elif req_type == "read_change_req":
                    read_change_req = request.read_change_req
                    cursor = read_change_req.cursor
                    if cursor.WhichOneof('cursor') != "column_cursor":
                        raise ValueError("cursor error: only 'column_cursor' iterating by files expected")
                    frm = getattr(cursor.column_cursor.data_range, 'from')
                    if frm.WhichOneof('data') != "string":
                        raise ValueError(
                            "cursor error: only 'string' type of column as file name iterating is expected")
                    last_key = frm.string

                    # Step 1: poll all files for table, sort them, and bisect with last_filename
                    s3_keys = sorted(list_s3_keys(params, table.name))
                    if not s3_keys:
                        raise ValueError(f"No S3 keys representing object {table.name}")
                    id = bisect.bisect_right(s3_keys, last_key)
                    if id < len(s3_keys) and s3_keys[id] == last_key:
                        id += 1
                    if id >= len(s3_keys):
                        # that's over, send end cursor
                        yield src.ReadRsp(result=mkOk(),
                                          read_ctl_rsp=read_ctl.ReadCtlRsp(read_change_rsp=read_ctl.ReadChangeRsp(
                                              checkpoint=read_ctl.ReadChangeRsp.CheckPoint(
                                                  cursor=common.Cursor(
                                                      end_cursor=common.EndCursor()
                                                  )
                                              )
                                          )))
                        continue
                    next_key = s3_keys[id]
                    # Step 2: read file and convert to change items
                    yield from produceChangeItems(params, next_key)
                elif req_type == "begin_snapshot_req":
                    yield src.ReadRsp(result=mkOk(),
                                      read_ctl_rsp=read_ctl.ReadCtlRsp(begin_snapshot_rsp=read_ctl.BeginSnapshotRsp(
                                          snapshot_state=b''
                                      )))
                elif req_type == "done_snapshot_req":
                    yield src.ReadRsp(result=mkOk(),
                                      read_ctl_rsp=read_ctl.ReadCtlRsp(done_snapshot_rsp=read_ctl.DoneSnapshotRsp(
                                      )))
                else:
                    raise ValueError(f"unknown control type: {req_type}")
            except Exception as e:
                print(traceback.format_exc())
                yield src.ReadRsp(result=mkErr(str(e)))

    def Stream(self, request_iterator, context):
        raise NotImplementedError("Stream protocol is not applicable to this source")
