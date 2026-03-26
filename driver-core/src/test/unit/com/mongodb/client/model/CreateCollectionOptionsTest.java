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

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class CreateCollectionOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        CreateCollectionOptions options = new CreateCollectionOptions();

        assertNull(options.getCollation());
        assertNull(options.getIndexOptionDefaults().getStorageEngine());
        assertEquals(0, options.getMaxDocuments());
        assertEquals(0, options.getSizeInBytes());
        assertNull(options.getStorageEngineOptions());
        assertNull(options.getValidationOptions().getValidator());
        assertFalse(options.isCapped());
    }

    @Test
    void shouldSetCollation() {
        assertNull(new CreateCollectionOptions().collation(null).getCollation());
        assertEquals(Collation.builder().locale("en").build(),
                new CreateCollectionOptions().collation(Collation.builder().locale("en").build()).getCollation());
    }

    @Test
    void shouldSetIndexOptionDefaults() {
        IndexOptionDefaults indexOptionDefaults = new IndexOptionDefaults()
                .storageEngine(BsonDocument.parse("{ storageEngine: { mmapv1 : {} }}"));
        assertEquals(indexOptionDefaults,
                new CreateCollectionOptions().indexOptionDefaults(indexOptionDefaults).getIndexOptionDefaults());
    }

    @Test
    void shouldSetMaxDocuments() {
        assertEquals(-1, new CreateCollectionOptions().maxDocuments(-1).getMaxDocuments());
        assertEquals(0, new CreateCollectionOptions().maxDocuments(0).getMaxDocuments());
        assertEquals(1, new CreateCollectionOptions().maxDocuments(1).getMaxDocuments());
    }

    @Test
    void shouldSetSizeInBytes() {
        assertEquals(-1, new CreateCollectionOptions().sizeInBytes(-1).getSizeInBytes());
        assertEquals(0, new CreateCollectionOptions().sizeInBytes(0).getSizeInBytes());
        assertEquals(1, new CreateCollectionOptions().sizeInBytes(1).getSizeInBytes());
    }

    @Test
    void shouldSetStorageEngineOptions() {
        assertNull(new CreateCollectionOptions().storageEngineOptions(null).getStorageEngineOptions());
        assertEquals(BsonDocument.parse("{ mmapv1 : {} }"),
                new CreateCollectionOptions().storageEngineOptions(BsonDocument.parse("{ mmapv1 : {} }")).getStorageEngineOptions());
    }

    @Test
    void shouldSetValidationOptions() {
        ValidationOptions validationOptions = new CreateCollectionOptions()
                .validationOptions(new ValidationOptions()).getValidationOptions();
        assertNull(validationOptions.getValidator());
        assertNull(validationOptions.getValidationLevel());
        assertNull(validationOptions.getValidationAction());

        ValidationOptions validationOptionsWithAction = new CreateCollectionOptions()
                .validationOptions(new ValidationOptions().validationAction(ValidationAction.ERROR))
                .getValidationOptions();
        assertNull(validationOptionsWithAction.getValidator());
        assertNull(validationOptionsWithAction.getValidationLevel());
        assertEquals(ValidationAction.ERROR, validationOptionsWithAction.getValidationAction());
    }

    @Test
    void shouldSetCapped() {
        assertEquals(true, new CreateCollectionOptions().capped(true).isCapped());
        assertEquals(false, new CreateCollectionOptions().capped(false).isCapped());
    }
}
