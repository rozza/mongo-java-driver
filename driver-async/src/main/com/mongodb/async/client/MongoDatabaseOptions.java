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

package com.mongodb.async.client;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.RootCodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * Default options for a Mongo collection.
 *
 * @since 3.0
 */
public class MongoDatabaseOptions {
    private final WriteConcern writeConcern;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;

    public static Builder builder() {
        return new Builder();
    }

    public MongoDatabaseOptions withDefaults(final MongoClientSettings settings) {
        Builder builder = new Builder();
        builder.writeConcern = getWriteConcern() != null ? getWriteConcern() : settings.getWriteConcern();
        builder.readPreference = getReadPreference() != null ? getReadPreference() : settings.getReadPreference();
        builder.codecRegistry(getCodecRegistry() != null ? getCodecRegistry()
                                                         : new RootCodecRegistry(asList(new DocumentCodecProvider())));
        return builder.build();
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    public static class Builder {
        private WriteConcern writeConcern;
        private ReadPreference readPreference;
        private CodecRegistry codecRegistry;

        public WriteConcern getWriteConcern() {
            return writeConcern;
        }

        public ReadPreference getReadPreference() {
            return readPreference;
        }

        public CodecRegistry getCodecRegistry() {
            return codecRegistry;
        }

        public Builder writeConcern(final WriteConcern writeConcern) {
            this.writeConcern = notNull("writeConcern", writeConcern);
            return this;
        }

        public Builder readPreference(final ReadPreference readPreference) {
            this.readPreference = notNull("readPreference", readPreference);
            return this;
        }

        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            this.codecRegistry = notNull("codecRegistry", codecRegistry);
            return this;
        }

        public MongoDatabaseOptions build() {
            return new MongoDatabaseOptions(writeConcern, readPreference, codecRegistry);
        }

        Builder() {
        }
    }

    MongoDatabaseOptions(final WriteConcern writeConcern,
                         final ReadPreference readPreference,
                         final CodecRegistry codecRegistry) {

        this.writeConcern = writeConcern;
        this.readPreference = readPreference;
        this.codecRegistry = codecRegistry;
    }
}
