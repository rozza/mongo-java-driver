/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.client.gridfs;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.MongoClient;
import com.mongodb.MongoGridFSException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

class GridFSFindIterableImpl implements GridFSFindIterable {
    private static final CodecRegistry DEFAULT_CODEC_REGISTRY = MongoClient.getDefaultCodecRegistry();
    private final FindIterable<BsonDocument> underlying;

    public GridFSFindIterableImpl(final FindIterable<BsonDocument> underlying) {
        this.underlying = underlying;
    }

    @Override
    public GridFSFindIterable sort(final Bson sort) {
        underlying.sort(sort);
        return this;
    }

    @Override
    public GridFSFindIterable skip(final int skip) {
        underlying.skip(skip);
        return this;
    }

    @Override
    public GridFSFindIterable limit(final int limit) {
        underlying.limit(limit);
        return this;
    }

    @Override
    public GridFSFindIterable filter(final Bson filter) {
        underlying.filter(filter);
        return this;
    }

    @Override
    public GridFSFindIterable maxTime(final long maxTime, final TimeUnit timeUnit) {
        underlying.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public GridFSFindIterable batchSize(final int batchSize) {
        underlying.batchSize(batchSize);
        return this;
    }

    @Override
    public GridFSFindIterable noCursorTimeout(final boolean noCursorTimeout) {
        underlying.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public MongoCursor<GridFSFile> iterator() {
        return toGridFSFileIterable().iterator();
    }

    @Override
    public GridFSFile first() {
        return toGridFSFileIterable().first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<GridFSFile, U> mapper) {
        return toGridFSFileIterable().map(mapper);
    }

    @Override
    public void forEach(final Block<? super GridFSFile> block) {
        toGridFSFileIterable().forEach(block);
    }

    @Override
    public <A extends Collection<? super GridFSFile>> A into(final A target) {
        return toGridFSFileIterable().into(target);
    }

    @SuppressWarnings("unchecked")
    private MongoIterable<GridFSFile> toGridFSFileIterable() {
        return underlying.map(new Function<BsonDocument, GridFSFile>() {
            @Override
            public GridFSFile apply(final BsonDocument document) {
                BsonValue id = document.get("_id");
                String filename = document.getString("filename").getValue();
                long length = document.getInt64("length").getValue();
                int chunkSize = document.getInt32("chunkSize").getValue();
                Date uploadDate = new Date(document.getDateTime("uploadDate").getValue());
                String md5 = document.getString("md5").getValue();
                Document metadata = DEFAULT_CODEC_REGISTRY.get(Document.class)
                        .decode(new BsonDocumentReader(document.getDocument("metadata", new BsonDocument())),
                                DecoderContext.builder().build());

                boolean isLegacy = (!id.isObjectId() || document.containsKey("contentType") || document.containsKey("aliases"));
                if (isLegacy) {
                    String contentType = document.containsKey("contentType") ? document.getString("contentType").getValue() : null;
                    List<String> aliases = null;
                    if (document.containsKey("aliases")) {
                        aliases = new ArrayList<String>();
                        for (BsonValue bsonValue : document.getArray("aliases")) {
                            if (!bsonValue.isString()) {
                                throw new MongoGridFSException(format("Unsupported alias value in aliases: %s", bsonValue));
                            } else {
                                aliases.add(bsonValue.asString().getValue());
                            }
                        }
                    }
                    return new GridFSFile(id, filename, length, chunkSize, uploadDate, md5, metadata, contentType, aliases);
                } else {
                    return new GridFSFile(id, filename, length, chunkSize, uploadDate, md5, metadata);
                }
            }
        });
    }

}
