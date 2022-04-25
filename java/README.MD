# DTExtension Java library

A library for implementing Yandex Data Transfer endpoints in Java language.

### Profobuf dependency instruction
Copy main proto files from `/proto` folder to `/java/protobuf/src/main/proto` folder
either manually or invoking `make cp_java` in the root of the project
and then use Gradle to generate files in `protobuf` submodule