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

package util;

import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.file.Files.isDirectory;
import static java.util.stream.Collectors.toMap;
import static org.bson.assertions.Assertions.assertNotNull;

public final class JsonPoweredTestHelper {

    // TODO - migrate to JsonPoweredTestHelper.getTestDocuments
    public static BsonDocument getTestDocument(final File file) {
        return getTestDocument(file.toPath().toString());
    }

    public static BsonDocument getTestDocument(final Path path) {
        return getTestDocument(() -> {
            try {
                return Files.newInputStream(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static BsonDocument getTestDocument(final String resourcePath) {
        return getTestDocument(() -> JsonPoweredTestHelper.class.getResourceAsStream(resourcePath));
    }

    public static BsonDocument getTestDocument(final Supplier<InputStream> inputStreamSupplier) {
        return new BsonDocumentCodec().decode(new JsonReader(inputStreamToString(inputStreamSupplier)), DecoderContext.builder().build());
    }

    public static Path testDir(final String resourceName) {
        URL res = JsonPoweredTestHelper.class.getResource(resourceName);
        if (res == null) {
            throw new AssertionError("Did not find " + resourceName);
        }
        try {
            Path dir = Paths.get(res.toURI());
            if (!isDirectory(dir)) {
                throw new AssertionError(dir + " is not a directory");
            }
            return dir;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<Path, BsonDocument> testDocs(final Path dir) {
        PathMatcher jsonMatcher = FileSystems.getDefault().getPathMatcher("glob:**.json");
        try {
            return Files.list(dir)
                    .filter(jsonMatcher::matches)
                    .collect(toMap(Function.identity(), path -> getTestDocument(path.toFile())));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<BsonDocument> getTestDocuments(final String resourcePath) throws URISyntaxException, IOException {
        List<BsonDocument> files = new ArrayList<>();
        URI resource = assertNotNull(JsonPoweredTestHelper.class.getResource(resourcePath)).toURI();
        try (FileSystem fileSystem = (resource.getScheme().equals("jar") ? FileSystems.newFileSystem(resource, Collections.emptyMap()) : null)) {
            Path myPath = Paths.get(resource);
            Files.walkFileTree(myPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path filePath, final BasicFileAttributes attrs) throws IOException {
                    if (filePath.toString().endsWith(".json")) {
                        if (fileSystem == null) {
                            files.add(getTestDocument(filePath));
                        } else {
                            files.add(getTestDocument(filePath.toString()));
                        }
                    }
                    return super.visitFile(filePath, attrs);
                }
            });
        }
        return files;
    }

    public static List<File> getTestFiles(final String resourcePath) throws URISyntaxException, IOException {
        List<File> files = new ArrayList<>();
        URL resource = JsonPoweredTestHelper.class.getResource(resourcePath);
        File directory = new File(assertNotNull(resource).toExternalForm());
        addFilesFromDirectory(directory, files);
        return files;
    }

    private static String inputStreamToString(final Supplier<InputStream> inputStreamSupplier)  {
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        String ls = System.lineSeparator();
        try (InputStream inputStream = inputStreamSupplier.get()) {
            assertNotNull(inputStream);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                while ((line = reader.readLine()) != null) {
                    stringBuilder.append(line);
                    stringBuilder.append(ls);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stringBuilder.toString();
    }

    private static void addFilesFromDirectory(final File directory, final List<File> files) {
        String[] fileNames = directory.list();
        if (fileNames != null) {
            for (String fileName : fileNames) {
                File file = new File(directory, fileName);
                if (file.isDirectory()) {
                    addFilesFromDirectory(file, files);
                } else if (file.getName().endsWith(".json")) {
                    files.add(file);
                }
            }
        }
    }

    private JsonPoweredTestHelper() {
    }
}
