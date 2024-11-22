plugins {
    kotlin("jvm") version "2.0.21"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("com.azure:azure-cosmos:4.64.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.7.3")
    implementation("ch.qos.logback:logback-classic:1.2.11")
}

kotlin {
    jvmToolchain(22)
}

tasks.withType<JavaCompile> {
    targetCompatibility = "22"
    sourceCompatibility = "22"
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveClassifier.set("")
    manifest {
        attributes["Main-Class"] = "MainKt"
    }
    destinationDirectory.set(file("$buildDir/libs"))
}

tasks {
    build {
        dependsOn(shadowJar)
    }
    named("distZip") {
        dependsOn(shadowJar)
    }
    named("distTar") {
        dependsOn(shadowJar)
    }
    named("startScripts") {
        dependsOn(shadowJar)
    }
    named("startShadowScripts") {
        dependsOn(jar)
    }
}

application {
    mainClass.set("MainKt")
}
