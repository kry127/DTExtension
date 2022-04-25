cp_java:
	cp -r ./proto ./java/protobuf/src/main/proto

gen_java:
	protoc --proto_path=$(pwd) --grpc-java_out ./java/protobuf/src/main/java proto/api/v1/*.proto proto/api/v1/sink_protocol/*.proto proto/api/v1/source_protocol/*.proto

gen_python:
	protoc --proto_path=$(pwd) --python_out=./python proto/api/v1/*.proto proto/api/v1/sink_protocol/*.proto proto/api/v1/source_protocol/*.proto
