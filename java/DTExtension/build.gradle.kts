import org.gradle.kotlin.dsl.dependencies;

plugins {
    java
}

repositories {
    mavenCentral()
}

val protobufVersion = "3.20.1"

dependencies {
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("io.grpc:grpc-stub:1.45.1")
    implementation(project(":protobuf"))

    testImplementation("junit", "junit", "4.12")
}