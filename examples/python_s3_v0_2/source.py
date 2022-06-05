# proto imports
import csv
import io
import pathlib
import typing

import api.v0_2.source.source_service_pb2_grpc as src_grpc
import api.v0_2.source.source_service_pb2 as src
import api.v0_2.common_pb2 as common
import api.v0_2.data_pb2 as data

# common library imports
import json
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
    s3 = session.resource('s3')

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=params.bucket, Prefix=params.prefix)

    objects = {}
    for page in pages:
        for obj in page['Contents']:
            key = obj['Key'].removeprefix(params.prefix)
            if params.partitioning_type == PartitioningType.PLAIN_TABLES \
                    and params.plain_tables \
                    and params.plain_tables.depth > 0:
                keyPath = pathlib.PurePath(key)
                id = max(len(keyPath.parents) - 1 - params.plain_tables.depth, 0)
                key = str(keyPath.parents[id])
            if params.file_type == FileType.CSV:
                response = s3.get_object(Bucket=params.bucket, Key=obj['Key'])
                data = io.TextIOWrapper(response)
                line_width = -1
                for line in data:
                    reader = csv.reader([line], skipinitialspace=True)
                    for r in reader:
                        line_width = len(r)
                        break
                    break

                objSchema = objects.get(key)
                if objSchema and objSchema != CsvSchema(line_width):
                    raise ValueError("Incompatible schema")
                else:
                    objects[key] = CsvSchema(line_width)
            elif params.file_type == FileType.JSON_LINES:
                objSchema = objects.get(key)
                if objSchema and objSchema != JsonLinesSchema():
                    raise ValueError("Incompatible schema")
                else:
                    objects[key] = JsonLinesSchema()
    return objects


# this function converts discovered objects (from 'discover_objects' function) to table proto description
# TODO we can add byte counter to schema and
# snapshot optimization: https://alexwlchan.net/2019/02/working-with-large-s3-objects/
def convert_to_proto(params: SourceParams, objects: typing.Dict[str, typing.Union[CsvSchema, JsonLinesSchema]]):
    tables = []
    for key, schema in objects:
        if isinstance(schema, CsvSchema):
            tables.append(data.Table(
                namespace=data.Namespace(namespace=params.bucket),
                name=key,
                schema=data.Schema(
                    # TODO separate CSV with and without header
                    columns=[data.Column(
                        name=f"column${i}",
                        key=False,
                        type=data.COLUMN_TYPE_STRING)
                        for i in range(schema.amount)]
                )
            ))
        elif isinstance(schema, JsonLinesSchema):
            tables.append(data.Table(
                namespace=data.Namespace(namespace=params.bucket),
                name=key,
                schema=data.Schema(
                    # TODO separate CSV with and without header
                    columns=[data.Column(
                        name=f"value",
                        key=False,
                        type=data.COLUMN_TYPE_JSON)]
                )
            ))
        else:
            raise ValueError(f"Invalid schema: ${schema}")

    return tables


class RouteGuideServicer(src_grpc.SourceServiceServicer):
    def Spec(self, request, context):
        try:
            with open('source_spec.json') as f:
                spec = f.read()
            return common.SpecRsp(result=mkOk(), json_spec=spec)
        except Exception as e:
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
            return src.DiscoverRsp(result=mkErr(str(e)))

    def Read(self, request_iterator, context):
        # TODO implement
        return super().Read(request_iterator, context)

    def Stream(self, request_iterator, context):
        raise NotImplementedError("Stream protocol is not applicable to this source")
