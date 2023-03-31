plugins {
    java
    id("com.github.johnrengelman.shadow") version "6.1.0"
}

group = "de.inovex"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.commons:commons-text:1.9")
    implementation("com.opencsv:opencsv:5.6")
    implementation("org.apache.kafka:connect-api:3.4.0")
    implementation("org.apache.kafka:kafka-clients:3.4.0")
    implementation("org.apache.kafka:connect-json:3.4.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.3")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.3")
    implementation("junit:junit:4.13.1")
    implementation("org.slf4j:slf4j-api:2.0.6")


    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.mockito:mockito-core:4.3.1")
    testImplementation("com.github.tomakehurst:wiremock:2.27.2")
    testImplementation("org.testcontainers:kafka:1.16.3")
    testImplementation("io.rest-assured:rest-assured:5.0.0")
    testImplementation("org.sourcelab:kafka-connect-client:4.0.0")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}

java {
    toolchain {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
}
