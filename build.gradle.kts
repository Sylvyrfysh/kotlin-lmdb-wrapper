import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.internal.os.OperatingSystem
import org.jetbrains.dokka.gradle.DokkaTask

plugins {
    idea
    maven
    kotlin("jvm") version "1.3.50"
    id("org.jetbrains.dokka") version "0.10.0"
}

group = "com.nicholaspjohnson"
version = "0.1.0"

repositories {
    jcenter()
    mavenCentral()
}

dependencies {
    // Kotlin deps
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

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
        testRuntimeOnly("org.lwjgl", actName, classifier = lwjglNatives)
    }

    //Test Framework
    testImplementation("org.junit.jupiter", "junit-jupiter-api", "5.5.2")
    testRuntimeOnly("org.junit.jupiter", "junit-jupiter-engine", "5.5.2")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8" //we use J1.8 features
}

tasks.test {
    useJUnitPlatform()
}

val dokka by tasks.getting(DokkaTask::class) {
    outputFormat = "javadoc"
    outputDirectory = "$buildDir/javadoc"
    this.configuration.apply {
        jdkVersion = 8
        noStdlibLink = false
        noJdkLink = false
        includeNonPublic = false
        skipDeprecated = false
        reportUndocumented = true
        skipEmptyPackages = true
        targets = listOf("JVM")
        platform = "JVM"

        sourceLink {
            path = "src/main/kotlin"
            url = "https://github.com/Sylvyrfysh/kotlin-lmdb-wrapper/blob/master/src/main/kotlin"
            lineSuffix = "#L"
        }

        perPackageOption {
            prefix = "com.nicholaspjohnson.kotlinlmdbwrapper.rwps"
            includeNonPublic = true
        }
    }
}

val dokkaJar by tasks.creating(Jar::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Assembles Kotlin docs with Dokka"
    archiveClassifier.set("javadoc")
    from(dokka)
}
artifacts.add("archives", dokkaJar)

val sourcesJar by tasks.creating(Jar::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Assembles sources JAR"
    archiveClassifier.set("sources")
    from(sourceSets.getByName("main").allSource)
}
artifacts.add("archives", sourcesJar)