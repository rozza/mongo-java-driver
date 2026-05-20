/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import ProjectExtensions.configureJarManifest
import ProjectExtensions.configureMavenPublication
import de.undercouch.gradle.tasks.download.Download

plugins {
    id("project.java")
    alias(libs.plugins.download)
}

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(libs.jna)
}

configureMavenPublication {
    pom {
        name.set("MongoCrypt")
        description.set("MongoDB client-side crypto support")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "com.mongodb.crypt.capi"
    attributes["Bundle-Name"] = "MongoCrypt"
    attributes["Bundle-SymbolicName"] = "com.mongodb.crypt.capi"
    attributes["Import-Package"] = "org.slf4j.*;resolution:=optional,org.bson.*"
    attributes["-exportcontents"] = "com.mongodb.*;-noimport:=true"
    attributes["Private-Package"] = ""
}

/*
 * Download and verify libmongocrypt native libraries from GitHub releases.
 *
 * To update the version: change `downloadRevision` below. Tarball names and structure
 * are documented at https://github.com/mongodb/libmongocrypt/releases
 *
 * To use local libraries instead of downloading: pass -DjnaLibsPath=/path/to/libs
 *
 * Downloads are cached in build/jnaLibs/downloads/ and skipped if already present.
 * GPG signatures are verified against https://pgp.mongodb.com/libmongocrypt.pub
 */
val jnaDownloadsDir = rootProject.file("build/jnaLibs/downloads/").path
val jnaResourcesDir = rootProject.file("build/jnaLibs/resources/").path
val jnaLibPlatform: String =
    if (com.sun.jna.Platform.RESOURCE_PREFIX.startsWith("darwin")) "darwin" else com.sun.jna.Platform.RESOURCE_PREFIX
val jnaLibsPath: String = System.getProperty("jnaLibsPath", "${jnaResourcesDir}${jnaLibPlatform}")
val jnaResources: String = System.getProperty("jna.library.path", jnaLibsPath)

val downloadRevision = "1.18.1"
val githubBaseUrl = "https://github.com/mongodb/libmongocrypt/releases/download/$downloadRevision"
val gpgPublicKeyUrl = "https://pgp.mongodb.com/libmongocrypt.pub"

data class PlatformDownload(val tarballName: String, val jnaDir: String, val libPath: String)

val platforms: List<PlatformDownload> =
    listOf(
        PlatformDownload(
            "libmongocrypt-linux-x86_64-glibc_2_7-nocrypto-$downloadRevision",
            "linux-x86-64",
            "lib64/libmongocrypt.so"),
        PlatformDownload(
            "libmongocrypt-linux-s390x-glibc_2_7-nocrypto-$downloadRevision", "linux-s390x", "lib64/libmongocrypt.so"),
        PlatformDownload(
            "libmongocrypt-linux-ppc64le-glibc_2_17-nocrypto-$downloadRevision",
            "linux-ppc64le",
            "lib64/libmongocrypt.so"),
        PlatformDownload(
            "libmongocrypt-linux-arm64-glibc_2_17-nocrypto-$downloadRevision",
            "linux-aarch64",
            "lib64/libmongocrypt.so"),
        PlatformDownload("libmongocrypt-windows-x86_64-$downloadRevision", "win32-x86-64", "bin/mongocrypt.dll"),
        PlatformDownload("libmongocrypt-macos-universal-$downloadRevision", "darwin", "lib/libmongocrypt.dylib"))

sourceSets { main { java { resources { srcDirs(jnaResourcesDir) } } } }

val downloadTasks =
    platforms.map { platform ->
        tasks.register<Download>("download_${platform.jnaDir.replace("-", "_")}") {
            src(listOf("$githubBaseUrl/${platform.tarballName}.tar.gz", "$githubBaseUrl/${platform.tarballName}.asc"))
            dest(jnaDownloadsDir)
            overwrite(false)
            onlyIfModified(true)
            onlyIf {
                !file("$jnaDownloadsDir/${platform.tarballName}.tar.gz").exists() ||
                    !file("$jnaDownloadsDir/${platform.tarballName}.asc").exists()
            }
        }
    }

tasks.register<Download>("downloadGpgKey") {
    src(gpgPublicKeyUrl)
    dest("$jnaDownloadsDir/libmongocrypt.pub")
    overwrite(false)
    onlyIfModified(true)
    onlyIf { !file("$jnaDownloadsDir/libmongocrypt.pub").exists() }
}

tasks.register("verifySignatures") {
    dependsOn(downloadTasks)
    dependsOn("downloadGpgKey")
    doLast {
        val keyFile = file("$jnaDownloadsDir/libmongocrypt.pub")
        val gpgHome = file("$jnaDownloadsDir/gnupg")
        delete(gpgHome)
        gpgHome.mkdirs()

        val gpgArgs =
            listOf(
                "gpg",
                "--homedir",
                gpgHome.absolutePath,
                "--batch",
                "--no-default-keyring",
                "--keyring",
                "${gpgHome.absolutePath}/keyring.gpg",
                "--trust-model",
                "always",
                "--no-permission-warning",
                "--quiet")

        fun runGpg(args: List<String>) {
            val process = ProcessBuilder(gpgArgs + args).redirectErrorStream(true).start()
            val output = process.inputStream.bufferedReader().readText()
            val exitCode = process.waitFor()
            if (exitCode != 0) {
                throw GradleException("GPG command failed (exit code $exitCode): ${gpgArgs + args}\n$output")
            }
        }

        runGpg(listOf("--import", keyFile.absolutePath))

        platforms.forEach { platform ->
            val tarball = file("$jnaDownloadsDir/${platform.tarballName}.tar.gz")
            val signature = file("$jnaDownloadsDir/${platform.tarballName}.asc")
            runGpg(listOf("--verify", signature.absolutePath, tarball.absolutePath))
        }
    }
}

tasks.register("extractJnaLibs") {
    dependsOn("verifySignatures")
    doFirst {
        println("Cleaning up $jnaResourcesDir")
        delete(jnaResourcesDir)
    }
    doLast {
        platforms.forEach { platform ->
            val tarball = file("$jnaDownloadsDir/${platform.tarballName}.tar.gz")
            val targetDir = file("$jnaResourcesDir/${platform.jnaDir}")
            targetDir.mkdirs()
            copy {
                from(tarTree(resources.gzip(tarball.absolutePath))) {
                    include("**/${platform.libPath.substringAfterLast("/")}")
                    eachFile { path = name }
                }
                into(targetDir)
            }
            val extractedLib = file("${targetDir}/${platform.libPath.substringAfterLast("/")}")
            if (!extractedLib.exists() || extractedLib.length() == 0L) {
                throw GradleException("Failed to extract ${platform.libPath} from ${platform.tarballName}.tar.gz")
            }
        }
        println("jna.library.path contents: \n  ${fileTree(jnaResourcesDir).files.joinToString(",\n  ")}")
    }
}

// The `processResources` task (defined by the `java-library` plug-in) consumes files in the main
// source set.
// Add a dependency on `extractJnaLibs`. `extractJnaLibs` adds libmongocrypt libraries to the main
// source set.
tasks.processResources { mustRunAfter(tasks.named("extractJnaLibs")) }

tasks.register("downloadJnaLibs") {
    if (System.getProperty("jnaLibsPath") == null) {
        dependsOn("extractJnaLibs")
    }
}

tasks.test {
    systemProperty("jna.debug_load", "true")
    systemProperty("jna.library.path", jnaResources)
    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }

    doFirst {
        println("jna.library.path contents:")
        println(fileTree(jnaResources) { this.setIncludes(listOf("*.*")) }.files.joinToString(",\n  ", "  "))
    }
    dependsOn("downloadJnaLibs")
}

tasks.withType<AbstractPublishToMaven> {
    description =
        """$description
        | System properties:
        | =================
        |
        | jnaLibsPath    : Custom local JNA library path for inclusion into the build (rather than downloading from GitHub releases)
    """.trimMargin()
}

tasks.withType<Jar> {
    // NOTE this enables depending on the mongocrypt from driver-core
    dependsOn("downloadJnaLibs")
}
