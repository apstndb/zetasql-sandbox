import java.util.Arrays;
val kotlinVersion = "1.3.61"

/*
buildscript {
    repositories {
        maven {
            url = uri("https://plugins.gradle.org/m2/")
        }
    }
    dependencies {
        classpath("com.pedjak.gradle.plugins:dockerized-test:0.5.10")
    }
}
*/

// apply(plugin = "com.github.pedjak.dockerized-test")

plugins {
    java
    id("com.google.cloud.tools.jib") version "1.8.0"
    kotlin("jvm") version "1.3.61"
    id("com.github.pedjak.dockerized-test") version "0.5.10"
}

group = "dev.apstn"
version = "1.0-SNAPSHOT"

repositories {
    maven( url = "http://dl.bintray.com/pedjak/gradle-plugins")
    mavenCentral()

}
dependencies {
    // testCompile("junit", "junit", "4.12")
    implementation(group= "com.google.zetasql", name= "zetasql-client", version= "2020.01.1")
    implementation(group= "com.google.zetasql", name= "zetasql-types", version= "2020.01.1")
    implementation(group= "com.google.zetasql", name= "zetasql-jni-channel", version= "2020.01.1")
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
    }
}

tasks {
    test {
        val docker: com.pedjak.gradle.plugins.dockerizedtest.DockerizedTestExtension by extensions
        // val docker = (extensions["docker"] as com.pedjak.gradle.plugins.dockerizedtest.DockerizedTestExtension)
        docker.image = "openjdk:8"
    }
}
