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

package com.mongodb.client.gridfs;

import com.mongodb.ClusterFixture;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.internal.MongoDatabaseImpl;
import com.mongodb.client.internal.OperationExecutor;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class GridFSBucketsTest {

    private final ReadConcern readConcern = ReadConcern.DEFAULT;

    @Test
    @DisplayName("should create a GridFSBucket with default bucket name")
    void shouldCreateGridFSBucketWithDefaultBucketName() {
        MongoDatabaseImpl database = new MongoDatabaseImpl("db", mock(CodecRegistry.class), mock(ReadPreference.class),
                mock(WriteConcern.class), false, true, readConcern, JAVA_LEGACY, null,
                ClusterFixture.TIMEOUT_SETTINGS, mock(OperationExecutor.class));

        GridFSBucket gridFSBucket = GridFSBuckets.create(database);

        assertThat(gridFSBucket, isTheSameAs(new GridFSBucketImpl(database)));
    }

    @Test
    @DisplayName("should create a GridFSBucket with custom bucket name")
    void shouldCreateGridFSBucketWithCustomBucketName() {
        MongoDatabaseImpl database = new MongoDatabaseImpl("db", mock(CodecRegistry.class), mock(ReadPreference.class),
                mock(WriteConcern.class), false, true, readConcern, JAVA_LEGACY, null,
                ClusterFixture.TIMEOUT_SETTINGS, mock(OperationExecutor.class));
        String customName = "custom";

        GridFSBucket gridFSBucket = GridFSBuckets.create(database, customName);

        assertThat(gridFSBucket, isTheSameAs(new GridFSBucketImpl(database, customName)));
    }
}
