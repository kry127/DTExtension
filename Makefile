cp_java_v0.1:
	cp -r ./proto/api/v0_1 ./java/protobuf/src/main/proto/api/

gen_java:
	protoc --proto_path=$(pwd) --grpc-java_out ./java/protobuf/src/main/java proto/api/v1/*.proto proto/api/v1/sink_protocol/*.proto proto/api/v1/source_protocol/*.proto

gen_python:
	protoc --proto_path=$(pwd) --python_out=./python proto/api/v1/*.proto proto/api/v1/sink_protocol/*.proto proto/api/v1/source_protocol/*.proto
