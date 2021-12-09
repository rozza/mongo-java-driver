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
package project.settings

import org.gradle.api.Project
import org.gradle.api.Plugin
import org.gradle.api.tasks.compile.JavaCompile
import project.settings.Versions

/**
 * Java configuration plugin
 */
class Java: Plugin<Project> {
    override fun apply(project: Project) {
        project.pluginManager.apply("java-library")
        project.dependencies.add("compileOnly", "com.google.code.findbugs:jsr305:${Versions.findBugs}")
        project.dependencies.add("api", "org.slf4j:slf4j-api:${Versions.slf4j}")
        project.dependencies.add("testImplementation", "com.google.code.findbugs:jsr305:${Versions.findBugs}")

        // https://issues.apache.org/jira/browse/GROOVY-10194
        project.dependencies.add("testImplementation", "org.codehaus.groovy:groovy-all:3.0.9")

        project.tasks.withType(JavaCompile::class.java) { task ->
            task.options.setEncoding("ISO-8859-1")
            task.options.setFork(true)
            task.options.setDebug(true)
            task.options.setCompilerArgs(listOf("-Xlint:all"))
        }

    }


    /*

    /* Compiling */
    tasks.withType(AbstractCompile) {
        options.encoding = 'ISO-8859-1'
        options.fork = true
        options.debug = true
        options.compilerArgs = ['-Xlint:all']
    }


tasks.withType(GenerateModuleMetadata) {
    enabled = false
}
     */
}
