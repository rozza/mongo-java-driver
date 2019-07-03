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

package com.mongodb.client.internal;

import com.mongodb.MongoClientException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.crypt.capi.MongoCrypt;
import com.mongodb.crypt.capi.MongoCryptContext;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.crypt.capi.MongoDataKeyOptions;
import com.mongodb.crypt.capi.MongoExplicitEncryptOptions;
import com.mongodb.crypt.capi.MongoKeyDecryptor;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.crypt.capi.MongoCryptContext.State;

class Crypt implements Closeable {

    private final MongoCrypt mongoCrypt;
    private final CollectionInfoRetriever collectionInfoRetriever;
    private final CommandMarker commandMarker;
    private final KeyRetriever keyRetriever;
    private final KeyManagementService keyManagementService;
    private final boolean bypassAutoEncryption;

    /**
     * Create an instance to use for explicit encryption and decryption, and data key creation.
     *
     * @param mongoCrypt the mongoCrypt wrapper
     * @param keyRetriever the key retriever
     * @param keyManagementService the key management service
     */
    Crypt(final MongoCrypt mongoCrypt, final KeyRetriever keyRetriever, final KeyManagementService keyManagementService) {
        this(mongoCrypt, null, null, keyRetriever, keyManagementService, false);
    }

    /**
     * Create an instance to use for auto-encryption and auto-decryption.
     *
     * @param mongoCrypt the mongoCrypt wrapper
     * @param keyRetriever the key retriever
     * @param keyManagementService the key management service
     * @param collectionInfoRetriever the collection info retriever
     * @param commandMarker the command marker
     */
    Crypt(final MongoCrypt mongoCrypt, @Nullable final CollectionInfoRetriever collectionInfoRetriever,
          @Nullable final CommandMarker commandMarker, final KeyRetriever keyRetriever, final KeyManagementService keyManagementService,
          final boolean bypassAutoEncryption) {
        this.mongoCrypt = mongoCrypt;
        this.collectionInfoRetriever = collectionInfoRetriever;
        this.commandMarker = commandMarker;
        this.keyRetriever = keyRetriever;
        this.keyManagementService = keyManagementService;
        this.bypassAutoEncryption = bypassAutoEncryption;
    }

    /**
     * Encrypt the given command
     *
     * @param databaseName the namespace
     * @param command   the unencrypted command
     * @return the encyrpted command
     */
    public RawBsonDocument encrypt(final String databaseName, final RawBsonDocument command) {
        notNull("databaseName", databaseName);
        notNull("command", command);

        if (bypassAutoEncryption) {
            return command;
        }

        try {
            MongoCryptContext encryptionContext = mongoCrypt.createEncryptionContext(databaseName, command);

            try {
                return executeStateMachine(encryptionContext, databaseName);
            } finally {
                encryptionContext.close();
            }
        } catch (MongoCryptException e) {
            throw wrapInClientException(e);
        }
    }

    /**
     * Decrypt the given command response
     *
     * @param commandResponse the encrypted command response
     * @return the decrypted command response
     */
    RawBsonDocument decrypt(final RawBsonDocument commandResponse) {
        notNull("commandResponse", commandResponse);

        try {
            MongoCryptContext decryptionContext = mongoCrypt.createDecryptionContext(commandResponse);

            try {
                return executeStateMachine(decryptionContext, null);
            } finally {
                decryptionContext.close();
            }
        } catch (MongoCryptException e) {
            throw wrapInClientException(e);
        }
    }

    /**
     * Create a data key.
     *
     * @param kmsProvider the KMS provider to create the data key for
     * @param options     the data key options
     * @return the document representing the data key to be added to the key vault
     */
    BsonDocument createDataKey(final String kmsProvider, final DataKeyOptions options) {
        notNull("kmsProvider", kmsProvider);
        notNull("options", options);

        try {
            MongoCryptContext dataKeyCreationContext = mongoCrypt.createDataKeyContext(kmsProvider,
                    MongoDataKeyOptions.builder()
                            .keyAltNames(options.getKeyAltNames())
                            .masterKey(options.getMasterKey())
                            .build());

            try {
                return executeStateMachine(dataKeyCreationContext, null);
            } finally {
                dataKeyCreationContext.close();
            }
        } catch (MongoCryptException e) {
            throw wrapInClientException(e);
        }
    }

    /**
     * Encrypt the given value with the given options
     *
     * @param value the value to encrypt
     * @param options the options
     * @return the encrypted value
     */
    BsonBinary encryptExplicitly(final BsonValue value, final EncryptOptions options) {
        notNull("value", value);
        notNull("options", options);

        try {
            MongoExplicitEncryptOptions.Builder encryptOptionsBuilder = MongoExplicitEncryptOptions.builder()
                    .algorithm(options.getAlgorithm());

            if (options.getKeyId() != null) {
                encryptOptionsBuilder.keyId(options.getKeyId());
            }

            if (options.getKeyAltName() != null) {
                encryptOptionsBuilder.keyAltName(options.getKeyAltName());
            }

            MongoCryptContext encryptionContext = mongoCrypt.createExplicitEncryptionContext(
                    new BsonDocument("v", value), encryptOptionsBuilder.build());
            try {
                return executeStateMachine(encryptionContext, null).getBinary("v");
            } finally {
                encryptionContext.close();
            }
        } catch (MongoCryptException e) {
            throw wrapInClientException(e);
        }
    }

    /**
     * Decrypt the given encrypted value.
     *
     * @param value the encrypted value
     * @return the decrypted value
     */
    BsonValue decryptExplicitly(final BsonBinary value) {
        notNull("value", value);

        try {
            MongoCryptContext decryptionContext = mongoCrypt.createExplicitDecryptionContext(new BsonDocument("v", value));

            try {
                return executeStateMachine(decryptionContext, null).get("v");
            } finally {
                decryptionContext.close();
            }
        } catch (MongoCryptException e) {
            throw wrapInClientException(e);
        }
    }

    @Override
    public void close() {
        mongoCrypt.close();
        commandMarker.close();
        keyRetriever.close();
    }

    private RawBsonDocument executeStateMachine(final MongoCryptContext cryptContext, final String databaseName) {
        while (true) {
            State state = cryptContext.getState();
            switch (state) {
                case NEED_MONGO_COLLINFO:
                    BsonDocument collectionInfo = collectionInfoRetriever.filter(databaseName, cryptContext.getMongoOperation());
                    if (collectionInfo != null) {
                        cryptContext.addMongoOperationResult(collectionInfo);
                    }
                    cryptContext.completeMongoOperation();
                    break;
                case NEED_MONGO_MARKINGS:
                    RawBsonDocument markedCommand = commandMarker.mark(databaseName, cryptContext.getMongoOperation());
                    cryptContext.addMongoOperationResult(markedCommand);
                    cryptContext.completeMongoOperation();
                    break;
                case NEED_MONGO_KEYS:
                    fetchKeys(cryptContext);
                    break;
                case NEED_KMS:
                    decryptKeys(cryptContext);
                    break;
                case READY:
                    return (RawBsonDocument) cryptContext.finish();
                default:
                    throw new MongoInternalException("Unsupported encryptor state + " + state);
            }
        }
    }

    private void fetchKeys(final MongoCryptContext keyBroker) {
        Iterator<BsonDocument> iterator = keyRetriever.find(keyBroker.getMongoOperation());
        while (iterator.hasNext()) {
            keyBroker.addMongoOperationResult(iterator.next());
        }
        keyBroker.completeMongoOperation();
    }

    private void decryptKeys(final MongoCryptContext cryptContext) {
        MongoKeyDecryptor keyDecryptor = cryptContext.nextKeyDecryptor();
        while (keyDecryptor != null) {
            decryptKey(keyDecryptor);
            keyDecryptor = cryptContext.nextKeyDecryptor();
        }
        cryptContext.completeKeyDecryptors();
    }

    private void decryptKey(final MongoKeyDecryptor keyDecryptor) {
        InputStream inputStream = keyManagementService.stream(keyDecryptor.getHostName(), keyDecryptor.getMessage());
        try {
            byte[] bytes = new byte[4096];

            int bytesNeeded = keyDecryptor.bytesNeeded();

            while (bytesNeeded > 0) {
                int bytesRead = inputStream.read(bytes, 0, bytesNeeded);
                keyDecryptor.feed(ByteBuffer.wrap(bytes, 0, bytesRead));
                bytesNeeded = keyDecryptor.bytesNeeded();
            }
        } catch (IOException e) {
            throw new MongoSocketReadException("Exception receiving message from key management service",
                    new ServerAddress(keyDecryptor.getHostName(), keyManagementService.getPort()), e);
            // type
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private MongoClientException wrapInClientException(final MongoCryptException e) {
        return new MongoClientException("Exception in encryption library: " + e.getMessage(), e);
    }
}
