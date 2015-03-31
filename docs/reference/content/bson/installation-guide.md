+++
date = "2015-03-17T15:36:56Z"
title = "Installation Guide"
[menu.main]
  parent = "BSON"
  weight = 50
  pre = "<i class='fa'></i>"
+++

# Installation

The BSON library is a required dependency of all the MongoDB Java Drivers and if using a  dependency management system, it should be 
automatically installed alongside the driver. However, it can be used as a standalone library ad 

{{< distroPicker >}}


## BSON

This library comprehensively supports [BSON](http://www.bsonspec.org),
the data storage and network transfer format that MongoDB uses for "documents".
BSON is short for Binary [JSON](http://json.org/), is a binary-encoded serialization of JSON-like documents.

{{< install artifactId="bson" version="3.0.0-rc1" >}}
