+++
date = "2019-06-13T09:00:01+01:00"
title = "Field Level Encryption"
[menu.main]
  parent = "Sync Reference"
  identifier = "Sync Field Level Encryption"
  weight = 30
  pre = "<i class='fa'></i>"
+++

# Field Level Encryption

New in MongoDB 4.2 field level encryption allows administrators and developers to encrypt specific data fields in addition to other 
MongoDB encryption features.

With field level encryption, developers can encrypt fields client side without any server-side 
configuration or directives. Client-side field level encryption supports workloads where applications must guarantee that 
unauthorized parties, including server administrators, cannot read the encrypted data.

## Installation

The recommended way to get started using field level encryption in your project is with a dependency management system. 
Field level encryption requires additional packages to be installed as well as the driver itself.  
See the [installation]({{< relref "driver/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver. 

{{< distroPicker >}}

### libmongocrypt

There is a separate jar file containing`libmongocrypt` bindings.

{{< install artifactId="mongodb-mongocrypt" version="1.0.0-SNAPSHOT">}}

If the jar fails to run there are separate jar files for specific architectures:

#### RHEL 7.0*
{{< install artifactId="mongodb-mongocrypt" version="1.0.0-SNAPSHOT" classifier="linux64-rhel70">}}

#### OSX*
{{< install artifactId="mongodb-mongocrypt" version="1.0.0-SNAPSHOT" classifier="osx">}}

#### Windows*
{{< install artifactId="mongodb-mongocrypt" version="1.0.0-SNAPSHOT" classifier="win64">}}

#### Ubuntu 16.04
{{< install artifactId="mongodb-mongocrypt" version="1.0.0-SNAPSHOT" classifier="linux64-ubuntu1604">}}


* Distribution is included in the main `mongodb-mongocrypt` jar file.

### mongocryptd configuration

`libmongocrypt` requires the `mongocryptd` daemon / process to be running. A specific daemon / process uri can be configured in the 
`AutoEncryptionSettings` class by setting `mongocryptdURI` in the `extraOptions`.

{{% note class="important" %}}
If not configured the driver will automatically try to start the daemon / process in: `/tmp/mongocryptd.sock`. This requires
the `jnr.unixsocket` library to be installed.
{{% /note %}}

More information about libmongocrypt will soon be available from the official documentation.


### Examples

The following is a sample app that assumes key and schema have already been created in MongoDB and the `encryptedField` field is encrypted:

```java
import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.security.SecureRandom;
import java.util.Map;

public class ClientSideEncryptionSimpleTest {

    public static void main(String[] args) {

        // This would have to be the same master key as was used to create the encryption key
        var localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        var kmsProviders = Map.of("local", Map.<String, Object>of("key", localMasterKey));
        var keyVaultNamespace = "admin.datakeys";

        var autoEncryptionSettings = AutoEncryptionSettings.builder()
            .keyVaultNamespace(keyVaultNamespace)
            .kmsProviders(kmsProviders)
            .build();

        var clientSettings = MongoClientSettings.builder()
            .autoEncryptionSettings(autoEncryptionSettings)
            .build();

        var client = MongoClients.create(clientSettings);
        var collection = client.getDatabase("test").getCollection("coll");
        collection.drop(); // Clear old data

        collection.insertOne(new Document("encryptedField", "123456789"));

        System.out.println(collection.find().first().toJson());
    }
}
```

{{% note %}}
Auto encryption is an **enterprise** only feature.
{{% /note %}}

The following example shows how to configure the `AutoEncryptionSettings` instance and set the json schema map to be used:

```java
import com.mongodb.ConnectionString;
import com.mongodb.KeyVaultEncryptionSettings;
import com.mongodb.client.vault.KeyVaults;

...


var keyVaultNamespace = "admin.datakeys";
var keyVaultSettings = KeyVaultEncryptionSettings.builder()
        .keyVaultMongoClientSettings(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost"))
                .build())
        .keyVaultNamespace(keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .build();

var keyVault = KeyVaults.create(keyVaultSettings);
var dataKeyId = keyVault.createDataKey("local", new DataKeyOptions());
var base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());

var dbName = "test";
var collName = "coll";
var autoEncryptionSettings = AutoEncryptionSettings.builder()
    .keyVaultNamespace(keyVaultNamespace)
    .kmsProviders(kmsProviders)
    .namespaceToLocalSchemaDocumentMap(Map.of(dbName + "." + collName,
        // Need a schema that references the new data key
        BsonDocument.parse("{" +
                "  properties: {" +
                "    encryptedField: {" +
                "      encrypt: {" +
                "        keyId: [{" +
                "          \"$binary\": {" +
                "            \"base64\": \"" + base64DataKeyId + "\"," +
                "            \"subType\": \"04\"" +
                "          }" +
                "        }]," +
                "        bsonType: \"string\"," +
                "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\"" +
                "      }" +
                "    }" +
                "  }," +
                "  \"bsonType\": \"object\"" +
                "}"))
    ).build();
```

{{% note %}}
Auto encryption is an **enterprise** only feature.
{{% /note %}}

**Coming soon:** An example using the community version and demonstrating explicit encryption/decryption.
