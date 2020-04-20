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

package org.bson.codecs.pojo;

import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;

import static java.lang.String.format;

final class DiscriminatorLookup {
    private static final Map<String, List<Class<?>>> PACKAGE_CLASS_MAP = new ConcurrentHashMap<>();
    private final Map<String, Class<?>> discriminatorClassMap = new ConcurrentHashMap<>();
    private final Set<String> packages;

    DiscriminatorLookup(final Map<Class<?>, ClassModel<?>> classModels, final Set<String> packages) {
        for (ClassModel<?> classModel : classModels.values()) {
            if (classModel.getDiscriminator() != null) {
                discriminatorClassMap.put(classModel.getDiscriminator(), classModel.getType());
            }
        }
        this.packages = packages;
    }

    public Class<?> lookup(final String discriminator) {
        if (discriminatorClassMap.containsKey(discriminator)) {
            return discriminatorClassMap.get(discriminator);
        }

        Class<?> clazz = getClassForName(discriminator);
        if (clazz == null) {
            clazz = searchPackages(discriminator);
        }

        if (clazz == null) {
            throw new CodecConfigurationException(format("A class could not be found for the discriminator: '%s'.",  discriminator));
        } else {
            discriminatorClassMap.put(discriminator, clazz);
        }
        return clazz;
    }

    void addAutomaticClassModel(final ClassModel<?> classModel) {
        if (classModel.getDiscriminator() != null) {
            discriminatorClassMap.put(classModel.getDiscriminator(), classModel.getType());
            if (Modifier.isInterface(classModel.getType().getModifiers()) || Modifier.isAbstract(classModel.getType().getModifiers())) {
                String packageName = classModel.getType().getPackage() != null ? classModel.getType().getPackage().getName() : "";
                if (!packageName.isEmpty()) {
                    searchPackageForBsonDiscriminator(packageName);
                }
            }
        }
    }

    private Class<?> searchPackages(final String discriminator) {
        Class<?> clazz = null;
        for (String packageName : packages) {
            clazz = getClassForName(packageName + "." + discriminator);
            if (clazz != null) {
                return clazz;
            }
        }
        for (String packageName : packages) {
            searchPackageForBsonDiscriminator(packageName);
            if (discriminatorClassMap.containsKey(discriminator)) {
                return discriminatorClassMap.get(discriminator);
            }
        }
        return clazz;
    }

    private void searchPackageForBsonDiscriminator(final String packageName) {
        getClassesInPackageWithBsonDiscriminator(packageName)
                .forEach(c -> discriminatorClassMap.put(c.getAnnotation(BsonDiscriminator.class).value(), c));
    }

    private static Class<?> getClassForName(final String className) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (final ClassNotFoundException e) {
            // Ignore
        }
        return clazz;
    }

    private static List<Class<?>> getClassesInPackageWithBsonDiscriminator(final String packageName) {
        if (!PACKAGE_CLASS_MAP.containsKey(packageName)) {
            PACKAGE_CLASS_MAP.put(packageName,
                    getClassNamesInPackage(packageName).stream()
                    .map(DiscriminatorLookup::getClassForName)
                    .filter(c -> c != null && c.isAnnotationPresent(BsonDiscriminator.class))
                    .collect(Collectors.toList()));
        }
        return PACKAGE_CLASS_MAP.get(packageName);
    }

    private static List<String> getClassNamesInPackage(final String packageName) {
        String packagePath = packageName.replaceAll("\\.", File.separator);
        List<String> classNames = new ArrayList<>();
        String[] classPathEntries = System.getProperty("java.class.path", "").split(System.getProperty("path.separator", File.separator));

        for (String classpathEntry : classPathEntries) {
            try {
                if (classpathEntry.endsWith(".jar")) {
                    try (JarInputStream jarInputStream = new JarInputStream(new FileInputStream(new File(classpathEntry)))) {
                        JarEntry entry;
                        while ((entry = jarInputStream.getNextJarEntry()) != null) {
                            if (entry.getName().endsWith(".class") && entry.getName().contains(packagePath)) {
                                classNames.add(cleanClassName(entry.getName()));
                            }
                        }
                    }
                } else {
                    File[] files = new File(classpathEntry + File.separatorChar + packagePath).listFiles();
                    if (files != null) {
                        Arrays.stream(files).filter(f -> f.getName().endsWith(".class")).map(File::getName).forEach(n ->
                                classNames.add(cleanClassName(packageName + "." + n)));
                    }
                }
            } catch (Exception e) {
                // ignore
            }
        }
        return classNames;
    }

    private static String cleanClassName(final String className) {
        return className.substring(0, className.length() - 6).replaceAll("[\\|/]", ".");
    }
}
