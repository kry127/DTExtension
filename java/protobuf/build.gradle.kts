import com.google.protobuf.gradle.*

// import org.gradle.kotlin.dsl.provider.gradleKotlinDslOf

plugins {
    java
    idea
    id("com.google.protobuf") version "0.8.18"
}

group = "yandexcloud.datatransfer.dtextension"
version = "1.0-SNAPSHOT"
val protobufVersion = "3.20.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation("javax.annotation:javax.annotation-api:1.3.2")
    implementation("com.google.protobuf", "protoc", protobufVersion)
    implementation("io.grpc:protoc-gen-grpc-java:1.45.1")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("io.grpc:grpc-stub:1.45.1")
    implementation("io.grpc:grpc-protobuf:1.45.1")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.45.1"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc") {
                }
            }
        }
    }
    generatedFilesBaseDir = "$projectDir/src"
}
