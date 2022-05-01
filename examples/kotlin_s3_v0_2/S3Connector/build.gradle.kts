import org.gradle.kotlin.dsl.dependencies;

plugins {
    java
}

repositories {
    mavenCentral()
}

val protobufVersion = "3.20.1"
val grpcVersion = "1.45.1"
val kotlinGrpcVersion = "1.2.1"

dependencies {
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-kotlin-stub:$kotlinGrpcVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")
    implementation(project(":protobuf"))

    testImplementation("junit", "junit", "4.12")
}