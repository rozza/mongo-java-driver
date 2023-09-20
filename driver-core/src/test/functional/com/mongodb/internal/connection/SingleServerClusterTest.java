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

package com.mongodb.internal.connection;

import com.mongodb.LoggerSettings;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.binding.StaticBindingContext;
import com.mongodb.internal.selector.ServerAddressSelector;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;

import static com.mongodb.ClusterFixture.getCredential;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.getSecondary;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SingleServerClusterTest {
    private SingleServerCluster cluster;


    private void setUpCluster(final ServerAddress serverAddress) {
        SocketStreamFactory streamFactory = new SocketStreamFactory(SocketSettings.builder().build(),
                getSslSettings());
        ClusterId clusterId = new ClusterId();
        ClusterSettings clusterSettings = ClusterSettings.builder()
                .mode(ClusterConnectionMode.SINGLE)
                .hosts(singletonList(serverAddress))
                .build();
        cluster = new SingleServerCluster(clusterId,
                clusterSettings,
                new DefaultClusterableServerFactory(ServerSettings.builder().build(),
                        ConnectionPoolSettings.builder().maxSize(1).build(), InternalConnectionPoolSettings.builder().build(),
                        streamFactory, streamFactory, getCredential(),

                        LoggerSettings.builder().build(), null, null, null,
                        Collections.emptyList(), getServerApi(), null));
    }

    @After
    public void tearDown() {
        cluster.close();
    }

    @Test
    public void shouldGetDescription() {
        // given
        setUpCluster(getPrimary());

        // expect
        assertNotNull(cluster.getDescription());
    }

    @Test
    public void descriptionShouldIncludeSettings() {
        // given
        setUpCluster(getPrimary());

        // expect
        assertNotNull(cluster.getDescription().getClusterSettings());
        assertNotNull(cluster.getDescription().getServerSettings());
    }

    @Test
    public void shouldGetServerWithOkDescription() {
        // given
        setUpCluster(getPrimary());

        // when
        ServerTuple serverTuple = cluster.selectServer(clusterDescription -> getPrimaries(clusterDescription), new OperationIdContext());

        // then
        assertTrue(serverTuple.getServerDescription().isOk());
    }

    @Test
    public void shouldSuccessfullyQueryASecondaryWithPrimaryReadPreference() {
        // given
        ServerAddress secondary = getSecondary();
        setUpCluster(secondary);
        String collectionName = getClass().getName();
        Connection connection = cluster.selectServer(new ServerAddressSelector(secondary), new OperationIdContext()).getServer()
                .getConnection(new OperationIdContext());

        // when
        BsonDocument result = connection.command(getDefaultDatabaseName(), new BsonDocument("count", new BsonString(collectionName)),
                new NoOpFieldNameValidator(), ReadPreference.primary(), new BsonDocumentCodec(),
                new StaticBindingContext(NoOpSessionContext.INSTANCE, getServerApi(), IgnorableRequestContext.INSTANCE,
                        new OperationIdContext()));

        // then
        assertEquals(new BsonDouble(1.0).intValue(), result.getNumber("ok").intValue());
    }
}
