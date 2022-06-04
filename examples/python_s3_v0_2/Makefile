
py_proto:
	python -m grpc_tools.protoc -I./proto \
	  --python_out=. \
	  --grpc_python_out=. \
	  ./proto/api/v0_2/*.proto ./proto/api/v0_2/source/*.proto ./proto/api/v0_2/sink/*.proto
