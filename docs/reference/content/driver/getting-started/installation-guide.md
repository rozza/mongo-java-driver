+++
date = "2015-03-17T15:36:56Z"
title = "Installation Guide"
[menu.main]
  parent = "Sync Getting Started"
  identifier = "Sync Installation Guide"
  weight = 1
  pre = "<i class='fa'></i>"
+++

# Installation

There are two different MongoDB drivers available in the 3.0 release.  The preferred method is to use the new MongoDB Driver as a 
dependency, however, we still publish the legacy Uber Jar.
The recommended way to get started using one of the drivers in your project is with a dependency management system.

{{< distroPicker >}}

## MongoDB Driver  

The MongoDB Driver is the updated synchronous Java driver that includes the
legacy API as well as a new generic MongoCollection interface that complies with
a new cross-driver CRUD specification.

{{< install artifactId="mongodb-driver" version="3.0.0" >}}


## Uber MongoDB Java Driver
This is the legacy uber jar that contains everything you need; the BSON library, the core library and the mongodb-driver.

{{< install artifactId="mongo-java-driver" version="3.0.0" >}}
