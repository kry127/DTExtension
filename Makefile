cp_java_v0.1:
	cp -r ./proto/api/v0_1 ./lib/v0_1/java/protobuf/src/main/proto/api/

gen_java_v0.2:
	protoc --proto_path=./proto --grpc-java_out ./tmp proto/api/v0_2/*.proto proto/api/v0_2/source/*.proto proto/api/v0_2/sink/*.proto