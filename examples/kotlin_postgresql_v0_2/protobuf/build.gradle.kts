import com.google.protobuf.gradle.*

// import org.gradle.kotlin.dsl.provider.gradleKotlinDslOf

plugins {
    java
    idea
    kotlin("jvm") version "1.6.0"
    id("com.google.protobuf") version "0.8.18"
}

group = "yandexcloud.datatransfer.dtextension"
version = "1.0-SNAPSHOT"
val protobufVersion = "3.20.1"
val grpcVersion = "1.46.0"
val kotlinGrpcVersion = "1.2.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation("javax.annotation:javax.annotation-api:1.3.2")
    implementation("com.google.protobuf", "protoc", protobufVersion)
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-kotlin:$protobufVersion")
    implementation("io.grpc:protoc-gen-grpc-java:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-kotlin-stub:$kotlinGrpcVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }

        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:$kotlinGrpcVersion:jdk7@jar"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
                id("grpckt")
            }
            it.builtins {
                id("kotlin")
            }
        }
    }
    generatedFilesBaseDir = "$projectDir/src"
}
