+++
date = "2015-03-17T15:36:56Z"
draft = true
title = "Admin Quick Tour"
[menu.main]
  parent = "Getting Started"
  weight = 20
  pre = "<i class='fa'></i>"
+++

# MongoDB Driver Admin Quick Tour

This is the second part of the MongoDB driver quick tour. In the
[quick tour]({{< relref "getting-started/quick-tour.md" >}}) we looked at how to
use MongoDB with basic operations.  In this section we'll look at some of the
administrative features available in the driver.

The following code snippets come from the `QuickTourAdmin.java` example code
that can be found with the [driver
source]({{< srcref "driver/src/examples/tour/QuickTourAdmin.java">}}).

{{% note %}}
See the [installation guide]({{< relref "getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Driver.
{{% /note %}}

## Setup

To get use started we'll quickly connect and create a `mongoClient`, `database` and `collection`
variable for use in the examples below:

```java
MongoClient mongoClient = new MongoClient();
MongoDatabase database = mongoClient.getDatabase("mydb");
MongoCollection<Document> collection = database.getCollection("test");
```


## Get A List of Databases

You can get a list of the available databases:

```java
for (String name: mongoClient.listDatabaseNames()) {
    System.out.println(name);
}
```

Calling the `getDatabase()` on `MongoClient` does not create a database.
Only when a database is written to will a database be created. Examples
would be creating an index or collection or inserting a document into a
collection.

## Drop A Database

You can drop a database by name using a `MongoClient` instance:

```java
mongoClient.dropDatabase("databaseToBeDropped");
```

## Create A Collection

There are two ways to create a collection. Inserting a document will
create the collection if it doesn't exist or calling the
[createCollection](http://docs.mongodb.org/manual/reference/method/db.createCollection)
command.

An example of creating a capped collection\_ sized to 1 megabyte:

```java
database.createCollection("cappedCollection", new
CreateCollectionOptions().capped(true).sizeInBytes(0x100000));
```

## Get A List of Collections

You can get a list of the available collections in a database:

```java
for (String name : database.listCollectionNames()) {
    System.out.println(name);
}
```

## Drop A Collection

You can drop a collection by using the drop() method:

```java
collection.dropCollection();
```

And you should notice that the collection no longer exists.

## Create An Index

MongoDB supports indexes. To create an index, you just specify the field that
should be indexed,
and specify if you want the index to be ascending (`1`) or descending
(`-1`). The following creates an ascending index on the `i` field :

```java
// create an ascending index on the "i" field
 collection.createIndex(new Document("i", 1));
```

## Get a List of Indexes on a Collection

Use the `listIndexes()` method to get a list of indexes on a collection:
The following lists the indexes on the collection `test`

```java
for (final Document index : collection.listIndexes()) {
    System.out.println(index);
}
```

The example should print the following indexes:

```json
Document{{v=1, key=Document{{_id=1}}, name=_id_, ns=mydb.test}}
Document{{v=1, key=Document{{i=1}}, name=i_1, ns=mydb.test}}
```

## Text indexes

MongoDB also provides text indexes to support text search of string
content. Text indexes can include any field whose value is a string or
an array of string elements. To create a text index specify the string
literal "text" in the index document:

```java
// create a text index on the "content" field
coll.createIndex(new BasicDBObject("content", "text"));
```

As of MongoDB 2.6, text indexes are now integrated into the main query
language and enabled by default:

```java
// Insert some documents
collection.insertOne(new Document("_id", 0).append("content", "textual content"));
collection.insertOne(new Document("_id", 1).append("content", "additional content"));
collection.insertOne(new Document("_id", 2).append("content", "irrelevant content"));

// Find using the text index
Document search = new Document("$search", "textual content -irrelevant");
Document textSearch = new Document("$text", search);

long matchCount = collection.count(textSearch);
System.out.println("Text search matches: "+ matchCount);

// Find using the $language operator
textSearch = new Document("$text", search.append("$language", "english"));

matchCount = collection.count(textSearch);
System.out.println("Text search matches (english): "+ matchCount);

// Find the highest scoring match
Document projection = new Document("score", new Document("$meta", "textScore"));

myDoc = collection.find(textSearch).projection(projection).first();
System.out.println("Highest scoring document: "+ myDoc);
```

and it should print:

```json
Text search matches: 2
Text search matches (english): 2
Highest scoring document: Document{{_id=1, content=additional content, score=0.75}}
```

For more information about text search see the [text index]({{< docsref "/core/index-text" >}}) and
[$text query operator]({{< docsref "/reference/operator/query/text">}}) documentation.

That concludes the admin quick tour overview!  Remember any [command]({{< docsref "/reference/command">}}) that doesn't have a specific helper can be called by the [database.runCommand()](http://api.mongodb.org/java/3.0/?com/mongodb/async/client/MongoDatabase.html#runCommand-org.bson.conversions.Bson-com.mongodb.ReadPreference-com.mongodb.async.SingleResultCallback-).
