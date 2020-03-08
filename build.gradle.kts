import kotlin.collections.mapOf
val kotlinVersion = "1.3.61"

plugins {
    java
    id("com.google.cloud.tools.jib") version "1.8.0"
    kotlin("jvm") version "1.3.61"
}

group = "dev.apstn"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()

}
dependencies {
    implementation(group="com.google.cloud", name="google-cloud-bigquery", version="1.108.1")
    implementation(group= "com.google.zetasql", name= "zetasql-client", version= "2020.03.1")
    implementation(group= "com.google.zetasql", name= "zetasql-types", version= "2020.03.1")
    implementation(group= "com.google.zetasql", name= "zetasql-jni-channel", version= "2020.03.1")
    implementation(kotlin("stdlib-jdk8"))
    testImplementation("junit:junit:4.12")
    testImplementation(group="org.jetbrains.kotlin", name="kotlin-test-junit",version="1.3.61")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

jib {
    container {
        mainClass = "zetasql.Main"
        environment = mapOf(Pair("SUPPRESS_GCLOUD_CREDS_WARNING", "true"))
    }
}
