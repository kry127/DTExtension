# PostgreSQL connector written in Kotlin

A showcase of implementing connector to PostgreSQL data source

## Interactive schema generator
https://jsonschema.net/

## How to setup kotlin grpc generation
See: https://github.com/grpc/grpc-kotlin/blob/master/compiler/README.md

### Profobuf dependency instruction
Copy main proto files from `/proto` folder to `/examples/kotlin_postgresql_v0_2/protobuf/src/main/proto` folder
either manually or invoking `make cp_kotlin_postgresql_v0_2` in the root of the github project
and then use Gradle to generate files in `protobuf` submodule