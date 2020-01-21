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
    // testCompile("junit", "junit", "4.12")
    compile(group= "com.google.zetasql", name= "zetasql-client", version= "2020.01.1")
    compile(group= "com.google.zetasql", name= "zetasql-types", version= "2020.01.1")
    compile(group= "com.google.zetasql", name= "zetasql-jni-channel", version= "2020.01.1")
    implementation(kotlin("stdlib-jdk8"))
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_11
}

jib {
    container {
        mainClass = "Main"
    }
}
