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
package conventions

import com.diffplug.gradle.spotless.SpotlessApply
import com.diffplug.gradle.spotless.SpotlessCheck
import libs

// Spotless - keep your code spotless
// https://plugins.gradle.org/plugin/com.diffplug.spotless
plugins { alias(libs.plugins.spotless) }

val doesNotHaveACustomLicenseHeader = "/^(?s)(?!.*@custom-license-header).*/"

spotless {
    kotlinGradle {
        ktfmt().configure {
            it.setMaxWidth(120)
            it.setBlockIndent(4)
            it.setRemoveUnusedImports(true)
        }
        trimTrailingWhitespace()
        leadingTabsToSpaces()
        endWithNewline()
        licenseHeaderFile(rootProject.file("config/mongodb.license"), "(group|plugins|import|buildscript|rootProject)")
    }

    scala {
        target("**/*.scala")
        scalafmt("3.7.1").configFile(rootProject.file("config/scala/scalafmt.conf")).scalaMajorVersion("2.13")
    }

    kotlin {
        target("**/*.kt")
        ktfmt().configure {
            it.setMaxWidth(120)
            it.setBlockIndent(4)
            it.setRemoveUnusedImports(true)
        }
        trimTrailingWhitespace()
        leadingTabsToSpaces()
        endWithNewline()
        licenseHeaderFile(rootProject.file("config/mongodb.license"))
            .named("standard")
            .onlyIfContentMatches(doesNotHaveACustomLicenseHeader)
    }

    format("extraneous") {
        target("*.xml", "*.yml", "*.md")
        trimTrailingWhitespace()
        leadingTabsToSpaces()
        endWithNewline()
    }
}

tasks.named("check") { dependsOn("spotlessApply") }

tasks {
    withType<SpotlessApply>().configureEach {
        notCompatibleWithConfigurationCache("https://github.com/diffplug/spotless/issues/644")
    }
    withType<SpotlessCheck>().configureEach {
        notCompatibleWithConfigurationCache("https://github.com/diffplug/spotless/issues/644")
    }
}
