+++
date = "2015-03-17T15:36:56Z"
draft = false
title = "Quick Tour"
[menu.main]
  parent = "Getting Started"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# MongoDB Driver Quick Tour

The following code snippets come from the `QuickTour.java` example code
that can be found with the [driver
source]({{< srcref "driver/src/examples/tour/QuickTour.java">}}).

{{% note %}}
See the [installation guide]({{< relref "getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Driver.
{{% /note %}}

## Make a Connection

The following example shows five ways to connect to the
database `mydb` on the local machine. If the database does not exist, MongoDB
will create it for you.

```java
// To directly connect to a single MongoDB server
// (this will not auto-discover the primary even if it's a member of a replica set)
MongoClient mongoClient = new MongoClient();
// or
MongoClient mongoClient = new MongoClient( "localhost" );
// or
MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
// or, to connect to a replica set, with auto-discovery of the primary, supply a
seed list of members
MongoClient mongoClient = new MongoClient(Arrays.asList(new
ServerAddress("localhost", 27017),
                                      new ServerAddress("localhost", 27018),
                                      new ServerAddress("localhost", 27019)));
// or use a connection string
MongoClient mongoClient = new MongoClient(new
MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019"));

MongoDatabase database = mongoClient.getDatabase("mydb");
```

At this point, the `database` object will be a connection to a MongoDB
server for the specified database.

### MongoClient

The `MongoClient` instance actually represents a pool of connections
to the database; you will only need one instance of class
`MongoClient` even with multiple threads.

The `MongoClient` class is designed to be thread safe and shared among
threads. Typically you create only 1 instance for a given database
cluster and use it across your application.

{{% note class="important" %}}
When creating many `MongoClient` instances:

-   All resource usage limits (max connections, etc) apply per
    `MongoClient` instance
-   To dispose of an instance, make sure you call `MongoClient.close()`
    to clean up resources
{{% /note %}}

## Get a Collection

To get a collection to operate upon, specify the name of the collection to
the [getCollection(String collectionName)]({{< apiref "com/mongodb/client/MongoDatabase.html#getCollection-java.lang.String-">}})
method:

The following example gets the collection `test`. If the collection does not
exist, MongoDB will create it for you.

```java
MongoCollection<Document> collection = database.getCollection("test");
```

With this collection object, you can now do things like insert
data, query for data, etc.

## Insert a Document

Once you have the collection object, you can insert documents into the
collection. For example, consider the following JSON document; the document
contains a field `info` which is an embedded document:

``` javascript
{
   "name" : "MongoDB",
   "type" : "database",
   "count" : 1,
   "info" : {
               x : 203,
               y : 102
             }
}
```

To create the document using the Java driver, use the
[Document]({{< apiref "org/bson/Document.html">}}) class. You
can use this class to create the embedded document as well.

```java
Document doc = new Document("name", "MongoDB")
               .append("type", "database")
               .append("count", 1)
               .append("info", new Document("x", 203).append("y", 102));
```

To insert the document into the collection, use the `insertOne()` method.

```java
collection.insertOne(doc);
```

## Add Multiple Documents

To add multiple documents, you can use the `insertMany()` method.

The following example will add multiple documents of the form:

```javascript
{
   "i" : value
}
```

Create the documents in a loop.

```java
List<Document> documents = new ArrayList<Document>();
for (int i = 0; i < 100; i++) {
    documents.add(new Document("i", i));
}
```

To insert these documents to the collection, pass the list of documents to the
`insertMany()` method.

```java
collection.insertMany(documents);
```

## Count Documents in A Collection

Now that we've inserted 101 documents (the 100 we did in the loop, plus
the first one), we can check to see if we have them all using the
[count()]({{< apiref "com/mongodb/client/MongoCollection#count--">}})
method. The following code should print `101`.

```java
System.out.println(collection.count());
```

## Query the Collection

Use the
[find()]({{< apiref "com/mongodb/client/MongoCollection.html#find--">}})
method to query the collection.

### Find the First Document in a Collection

To get the first document in the collection, append the
[first()]({{< apiref "com/mongodb/client/MongoIterable.html#first--">}})
method to the
[find()]({{< apiref "com/mongodb/client/MongoCollection.html#find--">}})
operation. The `find()` with `first()` operation returns a
single document instead of a cursor. The use of `first()` can be useful for
queries that should only match a
single document, or if you are interested in the first document only.

The following example prints the first document found in the collection.

```java
Document myDoc = collection.find().first();
System.out.println(myDoc);
```

The example should print the following document:

```json
Document{{_id=54b5594843bb7b25f1c9da72, name=MongoDB, type=database, count=1,
info=Document{{x=203, y=102}}}}
```

{{% note %}}
The `_id` element has been added automatically by MongoDB to your
document and your value will differ from that shown. MongoDB reserves field
names that start with
"_" and "$" for internal use.
{{% /note %}}

### Find All Documents in a Collection

To retrieve all the documents in the collection, we will use the
`find()` method. The `find()` method returns a `MongoIterable` instance. Use the
`iterator()` method to get an iterator over the set of documents that matched
the
query and iterate. The following code retrieves all documents in the collection
and prints them out (101 documents):

```java
MongoCursor<Document> cursor = collection.find().iterator();
try {
    while (cursor.hasNext()) {
        System.out.println(cursor.next());
    }
} finally {
    cursor.close();
}
```

Although the following idiom is permissible, its use is discouraged as the
application can leak a cursor if the loop
terminates early:

```java
for (Document cur : collection.find()) {
    System.out.println(cur);
}
```

## Get A Single Document with a Query Filter

We can create a filter to pass to the find() method to get a subset of
the documents in our collection. For example, if we wanted to find the
document for which the value of the "i" field is 71, we would do the
following:

```java
import static com.mongodb.client.model.Filters.*;

myDoc = collection.find(eq("i", 71)).first();
System.out.println(myDoc);
```

and it should just print just one document

```json
Document{{_id=54b5629643bb7b2a52e19ea3, i=71}}
```


{{% note %}}
Use the [Filters]({{< apiref "com/mongodb/client/model/Filters">}}), [Sorts]({{< apiref "com/mongodb/client/model/Sorts">}}) and [Projections]({{< apiref "com/mongodb/client/model/Projections">}})
helpers for simple and concise ways of building up queries.
{{% /note %}}

## Get a Set of Documents with a Query

We can use the query to get a set of documents from our collection. For
example, if we wanted to get all documents where `"i" > 50`, we could
write:

```java
// now use a range query to get a larger subset
FindIterable<Document> iterable = collection.find(gt("i", 50));

for (Document document : iterable) {
    System.out.println(document);
}
```

which should print the documents where `i > 50`.

We could also get a range, say `50 < i <= 100`:

```java
iterable = collection.find(and(gt("i", 50), lte("i", 100)));

for (Document document : iterable) {
    System.out.println(document);
}
```

## Timeout Queries with MaxTime

MongoDB 2.6 introduced the ability to timeout individual queries:

```java
collection.find().maxTime(1, TimeUnit.SECONDS).first();
```

In the example above the maxTime is set to one second and the query will
be aborted after the full second is up.

## Bulk operations

Under the covers MongoDB is moving away from the combination of a write
operation followed by get last error (GLE) and towards a write commands
API. These new commands allow for the execution of bulk
insert/update/delete operations. There are two types of bulk operations:

1.  Ordered bulk operations.

      Executes all the operation in order and error out on the first write error.

2.   Unordered bulk operations.

      These operations execute all the operations in parallel and aggregates up all the errors.

      Unordered bulk operations do not guarantee order of execution.

Let's look at two simple examples using ordered and unordered
operations:

```java
// 2. Ordered bulk operation - order is guarenteed
collection.bulkWrite(Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                                   new InsertOneModel<>(new Document("_id", 5)),
                                   new InsertOneModel<>(new Document("_id", 6)),
                                   new UpdateOneModel<>(new Document("_id", 1),
                                                        new Document("$set", new
Document("x", 2))),
                                   new DeleteOneModel<>(new Document("_id", 2)),
                                   new ReplaceOneModel<>(new Document("_id", 3),
                                                         new Document("_id",
3).append("x", 4))));


 // 2. Unordered bulk operation - no guarantee of order of operation
collection.bulkWrite(Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                                   new InsertOneModel<>(new Document("_id", 5)),
                                   new InsertOneModel<>(new Document("_id", 6)),
                                   new UpdateOneModel<>(new Document("_id", 1),
                                                        new Document("$set", new
Document("x", 2))),
                                   new DeleteOneModel<>(new Document("_id", 2)),
                                   new ReplaceOneModel<>(new Document("_id", 3),
                                                         new Document("_id",
3).append("x", 4)))),
                     new BulkWriteOptions().ordered(false));
```

{{% note class="important" %}}
For servers older than 2.6 the API will down convert the operations,
and support the correct semantics for BulkWriteResult and
BulkWriteException each write operation has to be done one at a time.
It's not possible to down convert 100% so there might be slight edge
cases where it cannot correctly report the right numbers.
{{% /note %}}

The concludes the quick tour of the new driver. To learn about MongoDB
administration features available in the driver see the
[admin quick tour]({{< relref "getting-started/admin-quick-tour.md" >}}).
