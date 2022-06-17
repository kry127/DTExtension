import org.gradle.kotlin.dsl.dependencies;

plugins {
    java
    kotlin("jvm") version "1.6.20"
}

repositories {
    mavenCentral()
}

val protobufVersion = "3.20.1"
val grpcVersion = "1.46.0"
val kotlinGrpcVersion = "1.2.1"

dependencies {
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-kotlin-stub:$kotlinGrpcVersion") // gRPC generated code depends on this
    implementation("io.grpc:grpc-services:$grpcVersion") // for reflection API
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1") // for bidirectional flow
    implementation("net.pwall.json:json-kotlin-schema:0.34") // for JSON Schema validation
    implementation("com.beust:klaxon:5.5") // for deserializing JSON
    runtimeOnly("io.grpc:grpc-netty:$grpcVersion") // server for gRPC
    implementation(project(":protobuf")) // dependency on protobuf schemas project


    // connector-specific
    implementation("org.postgresql:postgresql:42.3.4") // for PostgreSQL JDBC + Replication API
    implementation("org.jetbrains.kotlinx:kotlinx-cli:0.3.4") // for CLI utility
    implementation("com.oracle.database.jdbc:ojdbc8:21.5.0.0") // for CLI utility

    testImplementation("junit", "junit", "4.12")
}