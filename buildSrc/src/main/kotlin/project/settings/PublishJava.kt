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
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.compile.JavaCompile
import project.settings.Versions
import org.gradle.api.publish.plugins.PublishingPlugin
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.jvm.tasks.Jar

/**
 * Java configuration plugin
 */
class PublishJava: Plugin<Project> {
    override fun apply(project: Project) {
        project.pluginManager.apply("maven-publish")
        project.pluginManager.apply("signing")

        project.tasks.register("sourcesJar", Jar::class.java, {
            it.from(project.getExtensions().getByType(JavaPluginExtension::class.java).sourceSets)
            it.archiveClassifier.set("sources")
        })

        project.tasks.register("javaDocJar", Jar::class.java, {
            //it.from(project.javadoc)
            it.archiveClassifier.set("javadoc")
        })

        project.extensions.configure<PublishingExtension>("publishing", {

            it.publications {
                it.create("maven", MavenPublication::class.java, {
                    it.artifactId = project.findProperty("archivesBaseName").toString()
                    it.from(project.components.getByName("java"))
                    it.artifact(project.getTasksByName("sourcesJar", false).first())
                    // it.artifact(project.getTasksByName("sourcesJar"))
                    //it.artifact(javadocJar)
                })
            }

            it.repositories.maven {
                it.name = "demo"
                it.url = project.uri(project.layout.buildDirectory.dir("repos/demo"))
            }
        })
    }
}


        //println(project.publishing.publications)

//        project.dependencies.add("compileOnly", "com.google.code.findbugs:jsr305:${Versions.findBugs}")
//        project.dependencies.add("api", "org.slf4j:slf4j-api:${Versions.slf4j}")
//        project.dependencies.add("testImplementation", "com.google.code.findbugs:jsr305:${Versions.findBugs}")
//
//        // https://issues.apache.org/jira/browse/GROOVY-10194
//        project.dependencies.add("testImplementation", "org.codehaus.groovy:groovy-all:3.0.9")
//
//        project.tasks.withType(JavaCompile::class.java) { task ->
//            task.options.setEncoding("ISO-8859-1")
//            task.options.setFork(true)
//            task.options.setDebug(true)
//            task.options.setCompilerArgs(listOf("-Xlint:all"))
//        }


/*
 apply plugin: 'maven-publish'
    apply plugin: 'signing'

    task sourcesJar(type: Jar) {
        from project.sourceSets.main.allJava
        classifier = 'sources'
    }

    task javadocJar(type: Jar) {
        from javadoc
        classifier = 'javadoc'
    }

    publishing {
        publications {
            mavenJava(MavenPublication) {
                artifactId = project.archivesBaseName
                from project.components.java
                artifact sourcesJar
                artifact javadocJar

            }
        }

        repositories configureMavenRepositories(project)
    }

    afterEvaluate {
        publishing.publications.mavenJava.artifactId = project.archivesBaseName
        publishing.publications.mavenJava.pom configurePom(project)
        signing {
            useInMemoryPgpKeys(findProperty("signingKey"), findProperty("signingPassword"))
            sign publishing.publications.mavenJava
        }
    }
 */
