/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.acceptancetest.index;

import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.operation.OrderBy;
import org.junit.Test;
import org.mongodb.Document;

import static com.mongodb.operation.OrderBy.ASC;
import static com.mongodb.operation.OrderBy.DESC;
import static com.mongodb.operation.OrderBy.fromInt;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Use cases for adding indexes to your MongoDB database via the Java driver.  Documents the index options that are currently supported by
 * the updated driver.
 */
public class AddIndexAcceptanceTest extends DatabaseTestCase {

    @Test
    public void shouldGetExistingIndexesOnDatabase() {
        collection.insertOne(new Document("new", "value"));

        assertThat("Should have the default index on _id when a document exists",
                   collection.getIndexes().size(), is(1));
        String nameOfIndex = (String) collection.getIndexes().get(0).get("name");
        assertThat("Should be the default index on id", nameOfIndex, is("_id_"));
    }

    @Test
    public void shouldCreateIndexOnCollectionWithoutIndex() {
        collection.createIndex(new Document("field", 1));

        assertThat("Should be default index and new index on the database now", collection.getIndexes().size(), is(2));
    }

    @Test
    public void shouldCreateIndexWithNameOfFieldPlusOrder() {
        collection.createIndex(new Document("field", 1));

        String nameOfCreatedIndex = (String) collection.getIndexes().get(1).get("name");
        assertThat("Should be an index with name of field, ascending", nameOfCreatedIndex, is("field_1"));
    }

    @Test
    public void shouldCreateAnAscendingIndex() {
        collection.createIndex(new Document("field", 1));

        Document newIndexDetails = collection.getIndexes().get(1);
        OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be ascending", order, is(ASC));
    }

    @Test
    public void shouldCreateADescendingIndex() {
        collection.createIndex(new Document("field", -1));

        Document newIndexDetails = collection.getIndexes().get(1);
        OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be descending", order, is(DESC));
    }

    @Test
    public void shouldCreateNonUniqueIndexByDefault() {
        collection.createIndex(new Document("field", 1));

        Document newIndexDetails = collection.getIndexes().get(1);
        assertThat("Index created should not be unique", newIndexDetails.get("unique"), is(nullValue()));
    }

    @Test
    public void shouldCreateIndexOfUniqueValues() {
        collection.createIndex(new Document("field", 1), new CreateIndexOptions().unique(true));

        Document newIndexDetails = collection.getIndexes().get(1);
        Boolean unique = (Boolean) newIndexDetails.get("unique");
        assertThat("Index created should be unique", unique, is(true));
    }

    @Test
    public void shouldSupportCompoundIndexes() {
        collection.createIndex(new Document("theFirstField", 1).append("theSecondField", 1));
        Document newIndexDetails = collection.getIndexes().get(1);

        Document keys = (Document) newIndexDetails.get("key");
        Object theFirstField = keys.get("theFirstField");
        assertThat("Index should contain the first key", theFirstField, is(notNullValue()));
        OrderBy orderBy = fromInt((Integer) theFirstField);
        assertThat("Index created should be ascending", orderBy, is(ASC));

        Object theSecondField = keys.get("theSecondField");
        assertThat("Index should contain the second key", theSecondField, is(notNullValue()));
        orderBy = fromInt((Integer) theSecondField);
        assertThat("Index created should be ascending", orderBy, is(ASC));

        assertThat("Index name should contain both field names", (String) newIndexDetails.get("name"),
                   is("theFirstField_1_theSecondField_1"));
    }

    @Test
    public void shouldSupportCompoundIndexesWithDifferentOrders() {
        collection.createIndex(new Document("theFirstField", 1).append("theSecondField", -1));

        Document newIndexDetails = collection.getIndexes().get(1);

        Document keys = (Document) newIndexDetails.get("key");

        OrderBy orderBy = fromInt((Integer) keys.get("theFirstField"));
        assertThat("First index should be ascending", orderBy, is(ASC));

        orderBy = fromInt((Integer) keys.get("theSecondField"));
        assertThat("Second index should be descending", orderBy, is(DESC));

        assertThat("Index name should contain both field names",
                   (String) newIndexDetails.get("name"),
                   is("theFirstField_1_theSecondField_-1"));
    }

    @Test
    public void shouldOnlyReturnIndexesForTheSelectedCollection() {
        collection.createIndex(new Document("field", 1));

        MongoCollection<Document> anotherCollection = database.getCollection("anotherCollection");
        anotherCollection.createIndex(new Document("someOtherField", 1));

        assertThat("Should be default index and new index on the first database", collection.getIndexes().size(), is(2));
        assertThat("Should be default index and new index on the second database", anotherCollection.getIndexes().size(), is(2));
    }

    @Test
    public void shouldBeAbleToAddGeoIndexes() {
        collection.createIndex(new Document("locationField", "2d"));
        assertThat("Should be default index and new index on the database now", collection.getIndexes().size(), is(2));
    }

    @Test
    public void shouldBeAbleToAddGeoSphereIndexes() {
        collection.createIndex(new Document("locationField", "2dsphere"));
        assertThat("Should be default index and new index on the database now", collection.getIndexes().size(), is(2));
    }

    @Test
    public void shouldSupportCompoundIndexesOfOrderedFieldsAndGeoFields() {
        collection.createIndex(new Document("locationField", "2d").append("someOtherField", 1));

        Document newIndexDetails = collection.getIndexes().get(1);

        Document keys = (Document) newIndexDetails.get("key");
        Object geoField = keys.get("locationField");
        assertThat("Index should contain the first key", geoField, is(notNullValue()));
        String geoIndexValue = geoField.toString();
        assertThat("Index created should be a geo index", geoIndexValue, is("2d"));

        Object orderedField = keys.get("someOtherField");
        assertThat("Index should contain the second key", orderedField, is(notNullValue()));
        OrderBy orderBy = fromInt((Integer) orderedField);
        assertThat("Index created should be ascending", orderBy, is(ASC));

        assertThat("Index name should contain both field names",
                   (String) newIndexDetails.get("name"),
                   is("locationField_2d_someOtherField_1"));
    }

    @Test
    public void shouldAllowAliasForIndex() {
        String indexAlias = "indexAlias";
        collection.createIndex(new Document("theField", 1), new CreateIndexOptions().name(indexAlias));

        String nameOfCreatedIndex = collection.getIndexes().get(1).getString("name");
        assertThat("Should be an index named after the alias", nameOfCreatedIndex, is(indexAlias));
    }

    @Test
    public void shouldCreateASparseIndex() {
        collection.createIndex(new Document("theField", 1), new CreateIndexOptions().sparse(true));

        Boolean sparse = collection.getIndexes().get(1).getBoolean("sparse");
        assertThat("Should be a sparse index", sparse, is(true));
    }

    @Test
    public void shouldCreateABackgroundIndex() {
        collection.createIndex(new Document("theField", 1), new CreateIndexOptions().background(true));

        Boolean background = collection.getIndexes().get(1).getBoolean("background");
        assertThat("Should be a background index", background, is(true));
    }

    @Test
    public void shouldCreateATtlIndex() {
        collection.createIndex(new Document("theField", 1), new CreateIndexOptions().expireAfterSeconds(1600));

        Integer ttl = collection.getIndexes().get(1).getInteger("expireAfterSeconds");
        assertThat("Should be a ttl index", ttl, is(1600));
    }

    //TODO: other ordering options
    //TODO: can you disable the index on ID for non-capped collections?
}
