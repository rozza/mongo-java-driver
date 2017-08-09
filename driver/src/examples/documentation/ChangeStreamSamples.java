/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package documentation;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FullDocument;
import org.bson.Document;

import java.util.List;

import static com.mongodb.Fixture.getDefaultDatabaseName;
import static com.mongodb.Fixture.getMongoClient;
import static java.lang.String.format;
import static java.util.Collections.singletonList;


public final class ChangeStreamSamples {

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     */
    public static void main(final String[] args) {
        MongoClient mongoClient;

        if (args.length == 0) {
            // Defaults to a localhost replicaset on ports: 27017, 27018, 27019
            mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019"));
        } else {
            mongoClient = new MongoClient(new MongoClientURI(args[0]));
        }

        MongoDatabase database = getMongoClient().getDatabase(getDefaultDatabaseName());
        MongoCollection<Document> collection = database.getCollection("changes");
        collection.insertOne(Document.parse("{test: 'a'}"));

        // 1. Create a simple change stream against an existing collection.
        ChangeStreamIterable<Document> changeStream = collection.watch();

        Document next = changeStream.iterator().tryNext();
        assert (next != null);
        System.out.println(format("1. Initial document from the Change Stream: %n %s", next.toJson()));

        // 2. Create a change stream with ‘lookup’ option enabled.
        changeStream = collection.watch().fullDocument(FullDocument.LOOKUP);

        next = changeStream.iterator().tryNext();
        assert (next != null);
        System.out.println(format("2. Document from the Change Stream, with lookup enabled: %n%s", next.toJson()));

         // 3. Create a change stream with ‘lookup’ option using a $match and ($redact or $project) stage
        List<Document> pipeline = singletonList(Document.parse("{ $match : { operationType: 'update'}}"));
        changeStream = collection.watch(pipeline).fullDocument(FullDocument.LOOKUP);

        next = changeStream.iterator().tryNext();
        assert (next == null);

        collection.updateOne(Document.parse("{test: 'a'}"), Document.parse("{$set: {test: 'b'}}"));

        next = changeStream.iterator().tryNext();
        assert (next != null);

        System.out.println(format("3. Document from the Change Stream, with lookup enabled, matching `update` operations only: "
                + "%n - UpdateDescription: %s"
                + "%n - Full response: %s", next.get("updateDescription", Document.class).toJson(), next.toJson()));


        // 4. Resume a change stream
        Document resumeToken = next.get("_id", Document.class);
        changeStream = collection.watch().resumeAfter(resumeToken);

        collection.insertOne(Document.parse("{test: 'c'}"));

        next = changeStream.iterator().tryNext();
        assert (next != null);
        System.out.println(format("4. Document from the Change Stream including a resume token:%n %s", next.toJson()));
    }

    private ChangeStreamSamples() {
    }
}
