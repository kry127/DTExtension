cp_java_v0_1:
	cp -r ./proto/api/v0_1 ./lib/v0_1/java/protobuf/src/main/proto/api/

gen_java_v0_2:
	protoc --proto_path=./proto --grpc-java_out ./tmp proto/api/v0_2/*.proto proto/api/v0_2/source/*.proto proto/api/v0_2/sink/*.proto

gen_golang_v0_2:
	protoc --proto_path=./proto\
		--go_out ./go/pkg  --go_opt paths=source_relative\
		--go-grpc_out ./go/pkg --go-grpc_opt paths=source_relative\
		proto/api/v0_2/*.proto proto/api/v0_2/source/*.proto proto/api/v0_2/sink/*.proto

cp_kotlin_postgresql_v0_2:
	cp -r ./proto/api/v0_2 ./examples/kotlin_postgresql_v0_2/protobuf/src/main/proto/api

cp_python_s3_v0_2:
	cp -r ./proto/ ./examples/python_s3_v0_2/proto/
