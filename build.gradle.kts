import kotlin.collections.mapOf
val kotlinVersion = "1.3.61"
val zetasqlVersion = "2020.06.1"

plugins {
    java
    id("com.google.cloud.tools.jib") version "1.8.0"
    kotlin("jvm") version "1.3.61"
    application
}

group = "dev.apstn"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("com.google.cloud:libraries-bom:3.0.0"))

    implementation(group="com.google.cloud", name="google-cloud-bigquery")
    implementation(group="com.google.cloud", name="google-cloud-spanner")
    runtimeOnly(group="io.grpc", name="grpc-netty")

    implementation(group= "com.google.zetasql", name= "zetasql-client", version=zetasqlVersion)
    implementation(group= "com.google.zetasql", name= "zetasql-types", version=zetasqlVersion)
    implementation(group= "com.google.zetasql", name= "zetasql-jni-channel", version=zetasqlVersion)
    implementation(kotlin("stdlib-jdk8"))
    testImplementation("junit:junit:4.12")
    testImplementation(group="org.jetbrains.kotlin", name="kotlin-test-junit",version="1.3.61")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

jib {
    container {
        mainClass = "dev.apstn.zetasql.Main"
        environment = mapOf("SUPPRESS_GCLOUD_CREDS_WARNING" to "true")
    }
}

application {
    mainClassName = "dev.apstn.zetasql.Main"
}

