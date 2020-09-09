import org.gradle.internal.os.OperatingSystem
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.net.URL

buildscript {
    repositories { jcenter() }

    dependencies {
        classpath(kotlin("gradle-plugin", "1.4.0"))
        classpath(kotlin("serialization", "1.4.0"))
    }
}

plugins {
    idea
    maven
    kotlin("jvm") version "1.4.0"
    kotlin("plugin.serialization") version "1.4.0"
    id("org.jetbrains.dokka") version "1.4.0"
}

class SemVerBuilder {
    var major = 0
    var minor = 0
    var patch = 0
    var preRelease: String? = null
    val buildIdentifier = ArrayList<String>()
}

fun semver(block: SemVerBuilder.() -> Unit): String {
    val svb = SemVerBuilder()
    block(svb)
    return buildString {
        append(svb.major)
        append('.')
        append(svb.minor)
        append('.')
        append(svb.patch)
        if (!svb.preRelease.isNullOrBlank()) {
            append('-')
            append(svb.preRelease)
        }
        if (svb.buildIdentifier.isNotEmpty()) {
            append('+')
            append(svb.buildIdentifier[0])
            for (i in 1 until svb.buildIdentifier.size) {
                append('.')
                append(svb.buildIdentifier[i])
            }
        }
    }
}

group = "com.nicholaspjohnson"
version = semver {
    major = 0
    minor = 4
    patch = 0
    preRelease = "dev"
    buildIdentifier += "KXSer"
    buildIdentifier += "PreRC"
}

repositories {
    jcenter()
    mavenCentral()
}

dependencies {
    // Kotlin deps
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

    implementation("org.jetbrains.kotlinx", "kotlinx-serialization-core", "1.0.0-RC")
    implementation("org.jetbrains.kotlinx", "kotlinx-serialization-protobuf", "1.0.0-RC")

    // LWJGL Deps
    @Suppress("INACCESSIBLE_TYPE") val lwjglNatives = when (OperatingSystem.current()) {
        OperatingSystem.LINUX   -> System.getProperty("os.arch").let {
            if (it.startsWith("arm") || it.startsWith("aarch64"))
                "natives-linux-${if (it.contains("64") || it.startsWith("armv8")) "arm64" else "arm32"}"
            else
                "natives-linux"
        }
        OperatingSystem.MAC_OS  -> "natives-macos"
        OperatingSystem.WINDOWS -> if (System.getProperty("os.arch").contains("64")) "natives-windows" else "natives-windows-x86"
        else -> throw Error("Unrecognized or unsupported Operating system. Please set \"lwjglNatives\" manually")
    }

    //LWJGL
    implementation(platform("org.lwjgl:lwjgl-bom:3.2.3"))
    for (lwjgl in listOf("", "lmdb", "jemalloc")) {
        val actName = "lwjgl${if (lwjgl.isEmpty()) lwjgl else "-$lwjgl"}"
        implementation("org.lwjgl", actName)
        runtimeOnly("org.lwjgl", actName, classifier = lwjglNatives)
        testRuntimeOnly("org.lwjgl", actName, classifier = lwjglNatives)
    }

    //Test Framework
    testImplementation("org.junit.jupiter", "junit-jupiter-api", "5.6.2")
    testRuntimeOnly("org.junit.jupiter", "junit-jupiter-engine", "5.6.2")
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8" //we use J1.8 features
    }

    test {
        useJUnitPlatform()
    }

    dokkaJavadoc.configure {
        outputDirectory.set(buildDir.resolve("javadoc"))

    }

    withType<DokkaTask>().configureEach {
        dokkaSourceSets {
            configureEach {
                jdkVersion.set(8)
                noStdlibLink.set(false)
                noJdkLink.set(false)
                includeNonPublic.set(false)
                skipDeprecated.set(false)
                reportUndocumented.set(true)
                skipEmptyPackages.set(true)
                platform.set(org.jetbrains.dokka.Platform.jvm)

                sourceLink {
                    localDirectory.set(File("src/main/kotlin"))
                    remoteUrl.set(URL("https://github.com/Sylvyrfysh/kotlin-lmdb-wrapper/blob/master/src/main/kotlin"))
                    remoteLineSuffix.set("#L")
                }
            }
        }
    }
}
