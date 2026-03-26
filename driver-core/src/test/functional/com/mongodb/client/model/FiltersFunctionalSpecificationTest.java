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

package com.mongodb.client.model;

import com.mongodb.MongoQueryException;
import com.mongodb.OperationFunctionalSpecification;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FiltersFunctionalSpecificationTest extends OperationFunctionalSpecification {
    private final Document a = new Document("_id", 1).append("x", 1)
            .append("y", "a")
            .append("a", Arrays.asList(1, 2, 3))
            .append("a1", Arrays.asList(new Document("c", 1).append("d", 2), new Document("c", 2).append("d", 3)));

    private final Document b = new Document("_id", 2).append("x", 2)
            .append("y", "b")
            .append("a", Arrays.asList(3, 4, 5, 6))
            .append("a1", Arrays.asList(new Document("c", 2).append("d", 3), new Document("c", 3).append("d", 4)));

    private final Document c = new Document("_id", 3).append("x", 3)
            .append("y", "c")
            .append("z", true);

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().insertDocuments(a, b, c);
    }

    private List<Document> find(final Bson filter) {
        return getCollectionHelper().find(filter, new Document("_id", 1));
    }

    @Test
    void testEq() {
        assertEquals(Collections.singletonList(a), find(eq("x", 1)));
        assertEquals(Collections.singletonList(b), find(eq("_id", 2)));
        assertEquals(Collections.singletonList(b), find(eq(2)));
    }

    @Test
    void testNe() {
        assertEquals(Arrays.asList(b, c), find(ne("x", 1)));
    }

    @Test
    void testNot() {
        assertEquals(Arrays.asList(b, c), find(not(eq("x", 1))));
        assertEquals(Collections.singletonList(a), find(not(gt("x", 1))));
        assertEquals(Arrays.asList(b, c), find(not(regex("y", "a.*"))));

        Document dbref = Document.parse("{$ref: \"1\", $id: \"1\"}");
        Document dbrefDoc = new Document("_id", 4).append("dbref", dbref);
        getCollectionHelper().insertDocuments(dbrefDoc);
        assertEquals(Arrays.asList(a, b, c), find(not(eq("dbref", dbref))));

        getCollectionHelper().deleteOne(dbrefDoc);
        dbref.put("$db", "1");
        dbrefDoc.put("dbref", dbref);
        getCollectionHelper().insertDocuments(dbrefDoc);
        assertEquals(Arrays.asList(a, b, c), find(not(eq("dbref", dbref))));

        Document subDoc = Document.parse("{x: 1, b: 1}");
        getCollectionHelper().insertDocuments(new Document("subDoc", subDoc));
        assertEquals(Arrays.asList(a, b, c, dbrefDoc), find(not(eq("subDoc", subDoc))));

        assertThrows(MongoQueryException.class, () -> find(not(and(eq("x", 1), eq("x", 1)))));
    }

    @Test
    void testNor() {
        assertEquals(Arrays.asList(b, c), find(nor(eq("x", 1))));
        assertEquals(Collections.singletonList(c), find(nor(eq("x", 1), eq("x", 2))));
        assertEquals(Arrays.asList(a, b, c), find(nor(and(eq("x", 1), eq("y", "b")))));
    }

    @Test
    void testGt() {
        assertEquals(Arrays.asList(b, c), find(gt("x", 1)));
    }

    @Test
    void testLt() {
        assertEquals(Arrays.asList(a, b), find(lt("x", 3)));
    }

    @Test
    void testGte() {
        assertEquals(Arrays.asList(b, c), find(gte("x", 2)));
    }

    @Test
    void testLte() {
        assertEquals(Arrays.asList(a, b), find(lte("x", 2)));
    }

    @Test
    void testExists() {
        assertEquals(Collections.singletonList(c), find(exists("z")));
        assertEquals(Arrays.asList(a, b), find(exists("z", false)));
    }

    @Test
    void testOr() {
        assertEquals(Collections.singletonList(a), find(or(Collections.singletonList(eq("x", 1)))));
        assertEquals(Arrays.asList(a, b), find(or(Arrays.asList(eq("x", 1), eq("y", "b")))));
    }

    @Test
    void testAnd() {
        assertEquals(Collections.singletonList(a), find(and(Collections.singletonList(eq("x", 1)))));
        assertEquals(Collections.singletonList(a), find(and(Arrays.asList(eq("x", 1), eq("y", "a")))));
    }

    @Test
    void andShouldDuplicateClashingKeys() {
        assertEquals(Collections.singletonList(a), find(and(Arrays.asList(eq("x", 1), eq("x", 1)))));
    }

    @Test
    void andShouldFlattenMultipleOperatorsForSameKey() {
        assertEquals(Arrays.asList(a, b), find(and(Arrays.asList(gte("x", 1), lte("x", 2)))));
    }

    @Test
    void andShouldFlattenNested() {
        assertEquals(Collections.singletonList(c), find(and(Arrays.asList(and(Arrays.asList(eq("x", 3), eq("y", "c"))), eq("z", true)))));
        assertEquals(Collections.singletonList(c), find(and(Arrays.asList(and(Arrays.asList(eq("x", 3), eq("x", 3))), eq("z", true)))));
        assertEquals(Arrays.asList(b, c), find(and(Arrays.asList(gt("x", 1), gt("y", "a")))));
        assertEquals(Arrays.asList(a, b), find(and(Arrays.asList(lt("x", 4), lt("x", 3)))));
    }

    @Test
    void explicitAndWhenUsingNot() {
        assertEquals(Arrays.asList(a, b), find(and(Arrays.asList(lt("x", 3), not(lt("x", 1))))));
        assertEquals(Arrays.asList(a, b), find(and(Arrays.asList(lt("x", 5), gt("x", 0), not(gt("x", 2))))));
        assertEquals(Collections.singletonList(b), find(and(Arrays.asList(not(lt("x", 2)), lt("x", 4), not(gt("x", 2))))));
    }

    @Test
    void shouldRenderAll() {
        assertEquals(Collections.singletonList(a), find(all("a", Arrays.asList(1, 2))));
    }

    @Test
    void shouldRenderElemMatch() {
        assertEquals(Collections.singletonList(a), find(elemMatch("a", new Document("$gte", 2).append("$lte", 2))));
        assertEquals(Collections.singletonList(a), find(elemMatch("a1", and(eq("c", 1), gte("d", 2)))));
        assertEquals(Arrays.asList(a, b), find(elemMatch("a1", and(eq("c", 2), eq("d", 3)))));
    }

    @Test
    void shouldRenderIn() {
        assertEquals(Collections.singletonList(a), find(Filters.in("a", Arrays.asList(0, 1, 2))));
    }

    @Test
    void shouldRenderNin() {
        assertEquals(Arrays.asList(b, c), find(nin("a", Arrays.asList(1, 2))));
    }

    @Test
    void shouldRenderMod() {
        assertEquals(Collections.singletonList(b), find(mod("x", 2, 0)));
    }

    @Test
    void shouldRenderSize() {
        assertEquals(Collections.singletonList(b), find(size("a", 4)));
    }

    @Test
    void shouldRenderBitsAllClear() {
        Document bitDoc = Document.parse("{_id: 1, bits: 20}");
        getCollectionHelper().drop();
        getCollectionHelper().insertDocuments(bitDoc);
        assertEquals(Collections.singletonList(bitDoc), find(bitsAllClear("bits", 35)));
    }

    @Test
    void shouldRenderBitsAllSet() {
        Document bitDoc = Document.parse("{_id: 1, bits: 54}");
        getCollectionHelper().drop();
        getCollectionHelper().insertDocuments(bitDoc);
        assertEquals(Collections.singletonList(bitDoc), find(bitsAllSet("bits", 50)));
    }

    @Test
    void shouldRenderBitsAnyClear() {
        Document bitDoc = Document.parse("{_id: 1, bits: 50}");
        getCollectionHelper().drop();
        getCollectionHelper().insertDocuments(bitDoc);
        assertEquals(Collections.singletonList(bitDoc), find(bitsAnyClear("bits", 20)));
    }

    @Test
    void shouldRenderBitsAnySet() {
        Document bitDoc = Document.parse("{_id: 1, bits: 20}");
        getCollectionHelper().drop();
        getCollectionHelper().insertDocuments(bitDoc);
        assertEquals(Collections.singletonList(bitDoc), find(bitsAnySet("bits", 50)));
    }

    @Test
    void shouldRenderType() {
        assertEquals(Arrays.asList(a, b, c), find(type("x", BsonType.INT32)));
        assertEquals(Collections.emptyList(), find(type("x", BsonType.ARRAY)));
    }

    @Test
    void shouldRenderTypeWithStringRepresentation() {
        assertEquals(Arrays.asList(a, b, c), find(type("x", "number")));
        assertEquals(Collections.emptyList(), find(type("x", "array")));
    }

    @SuppressWarnings("deprecation")
    @Test
    void shouldRenderText() {
        getCollectionHelper().createIndex(new Document("y", "text"));

        Document textDocument = new Document("_id", 4).append("y", "mongoDB for GIANT ideas");
        getCollectionHelper().insertDocuments(textDocument);

        assertEquals(Collections.singletonList(textDocument), find(text("GIANT")));
        assertEquals(Collections.singletonList(textDocument),
                find(text("GIANT", new TextSearchOptions().language("english"))));
    }

    @Test
    void shouldRenderTextWith32Options() {
        getCollectionHelper().drop();
        getCollectionHelper().createIndex(new Document("desc", "text"), "portuguese");

        Document textDocument = new Document("_id", 1).append("desc", "mongodb para id\u00e9ias GIGANTES");
        getCollectionHelper().insertDocuments(textDocument);

        assertEquals(Collections.singletonList(textDocument), find(text("id\u00e9ias")));
        assertEquals(Collections.singletonList(textDocument), find(text("ideias", new TextSearchOptions())));
        assertEquals(Collections.singletonList(textDocument),
                find(text("ideias", new TextSearchOptions().caseSensitive(false).diacriticSensitive(false))));
        assertEquals(Collections.singletonList(textDocument),
                find(text("ID\u00e9IAS", new TextSearchOptions().caseSensitive(false).diacriticSensitive(true))));
        assertEquals(Collections.emptyList(),
                find(text("ideias", new TextSearchOptions().caseSensitive(true).diacriticSensitive(true))));
        assertEquals(Collections.emptyList(),
                find(text("id\u00e9ias", new TextSearchOptions().language("english"))));
    }

    @Test
    void shouldRenderRegex() {
        assertEquals(Collections.singletonList(a), find(regex("y", "a.*")));
        assertEquals(Collections.singletonList(a), find(regex("y", "a.*", "si")));
        assertEquals(Collections.singletonList(a), find(regex("y", Pattern.compile("a.*"))));
    }

    @Test
    void shouldRenderWhere() {
        assertEquals(Arrays.asList(a, b), find(where("Array.isArray(this.a)")));
    }

    @Test
    void testExpr() {
        assertEquals(Collections.singletonList(c), find(expr(Document.parse("{ $eq: [ \"$x\" , 3 ] } "))));
    }

    @Test
    void testJsonSchema() {
        assertEquals(Arrays.asList(b, c),
                find(jsonSchema(Document.parse("{ bsonType : \"object\", properties: { x : {type : \"number\", minimum : 2} } } "))));
    }

    @Test
    void emptyMatchesEverything() {
        assertEquals(Arrays.asList(a, b, c), find(empty()));
    }
}
