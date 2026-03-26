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

package com.mongodb;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Java port of the Groovy CustomMatchers from driver-core.
 * Provides a reflective matcher that compares all fields of two objects of the same class.
 */
final class CustomMatchers {

    private CustomMatchers() {
    }

    static <T> Matcher<T> isTheSameAs(final T expected) {
        return isTheSameAs(expected, Collections.emptyList());
    }

    static <T> Matcher<T> isTheSameAs(final T expected, final List<String> ignoreNames) {
        return new BaseMatcher<T>() {
            @Override
            public boolean matches(final Object actual) {
                return compare(expected, actual, ignoreNames);
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Operation has the same attributes " + expected.getClass().getName());
            }

            @Override
            public void describeMismatch(final Object item, final Description description) {
                describer(expected, item, ignoreNames, description);
            }
        };
    }

    private static boolean compare(final Object expected, final Object actual, final List<String> ignoreNames) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }
        if (!actual.getClass().getName().equals(expected.getClass().getName())) {
            return false;
        }
        for (Field field : getFields(actual.getClass())) {
            if (ignoreNames.contains(field.getName())) {
                continue;
            }
            field.setAccessible(true);
            Object actualValue;
            Object expectedValue;
            try {
                actualValue = field.get(actual);
                expectedValue = field.get(expected);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            if (nominallyTheSame(field.getName())) {
                if (actualValue == null || expectedValue == null) {
                    if (actualValue != expectedValue) {
                        return false;
                    }
                } else if (!actualValue.getClass().equals(expectedValue.getClass())) {
                    return false;
                }
            } else if (!objectsEqual(actualValue, expectedValue, ignoreNames)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean objectsEqual(final Object actualValue, final Object expectedValue, final List<String> ignoreNames) {
        if (actualValue == expectedValue) {
            return true;
        }
        if (actualValue == null || expectedValue == null) {
            return false;
        }
        if (!actualValue.equals(expectedValue)) {
            if (actualValue instanceof List && expectedValue instanceof List) {
                List<Object> actualList = (List<Object>) actualValue;
                List<Object> expectedList = (List<Object>) expectedValue;
                if (actualList.size() != expectedList.size()) {
                    return false;
                }
                for (int i = 0; i < actualList.size(); i++) {
                    if (!compare(actualList.get(i), expectedList.get(i), ignoreNames)) {
                        return false;
                    }
                }
                return true;
            } else if (actualValue.getClass().getName().startsWith("com.mongodb")
                    && actualValue.getClass().equals(expectedValue.getClass())) {
                return compare(actualValue, expectedValue, ignoreNames);
            }
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static void describer(final Object expected, final Object actual, final List<String> ignoreNames,
                                  final Description description) {
        if (expected == actual) {
            return;
        }
        if (expected == null || actual == null) {
            description.appendText("different values: " + expected + " != " + actual + ", ");
            return;
        }
        if (!actual.getClass().getName().equals(expected.getClass().getName())) {
            description.appendText("different classes: " + expected.getClass().getName()
                    + " != " + actual.getClass().getName() + ", ");
            return;
        }
        for (Field field : getFields(actual.getClass())) {
            if (ignoreNames.contains(field.getName())) {
                continue;
            }
            field.setAccessible(true);
            Object actualValue;
            Object expectedValue;
            try {
                actualValue = field.get(actual);
                expectedValue = field.get(expected);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            if (nominallyTheSame(field.getName())) {
                if (actualValue != null && expectedValue != null
                        && !actualValue.getClass().equals(expectedValue.getClass())) {
                    description.appendText("different classes in " + field.getName() + " : "
                            + expectedValue.getClass().getName() + " != " + actualValue.getClass().getName() + ", ");
                }
            } else if (actualValue == null || expectedValue == null) {
                if (actualValue != expectedValue) {
                    description.appendText("different values in " + field.getName() + " : "
                            + expectedValue + " != " + actualValue + "\n");
                }
            } else if (!actualValue.equals(expectedValue)) {
                if (actualValue instanceof List && expectedValue instanceof List) {
                    List<Object> actualList = (List<Object>) actualValue;
                    List<Object> expectedList = (List<Object>) expectedValue;
                    if (actualList.size() == expectedList.size()) {
                        for (int i = 0; i < actualList.size(); i++) {
                            if (!compare(actualList.get(i), expectedList.get(i), ignoreNames)) {
                                describer(actualList.get(i), expectedList.get(i), ignoreNames, description);
                            }
                        }
                    }
                } else if (actualValue.getClass().getName().startsWith("com.mongodb")
                        && actualValue.getClass().equals(expectedValue.getClass())) {
                    describer(actualValue, expectedValue, ignoreNames, description);
                }
                description.appendText("different values in " + field.getName() + " : "
                        + expectedValue + " != " + actualValue + "\n");
            }
        }
    }

    private static List<Field> getFields(final Class<?> clazz) {
        if (clazz == Object.class) {
            return new ArrayList<>();
        }
        List<Field> fields = getFields(clazz.getSuperclass());
        for (Field field : clazz.getDeclaredFields()) {
            if (!field.isSynthetic() && !Modifier.isStatic(field.getModifiers()) && !field.getName().contains("$")) {
                fields.add(field);
            }
        }
        return fields;
    }

    private static boolean nominallyTheSame(final String propertyName) {
        return Arrays.asList("decoder", "executor").contains(propertyName);
    }
}
