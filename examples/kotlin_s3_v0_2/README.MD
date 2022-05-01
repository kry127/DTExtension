# S3 connector written in Kotlin

A showcase of implementing connector to S3 data source

## How to setup kotlin grpc generation
See: https://github.com/grpc/grpc-kotlin/blob/master/compiler/README.md

### Profobuf dependency instruction
Copy main proto files from `/proto` folder to `/java/protobuf/src/main/proto` folder
either manually or invoking `make cp_java` in the root of the project
and then use Gradle to generate files in `protobuf` submodule