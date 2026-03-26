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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class CustomMatchers {

    private static final List<Object> NULL_LIST = Arrays.asList(null, null);

    public static <T> BaseMatcher<T> isTheSameAs(final T expected) {
        return isTheSameAs(expected, Collections.emptyList());
    }

    public static <T> BaseMatcher<T> isTheSameAs(final T expected, final List<String> ignoreNames) {
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
            public void describeMismatch(final Object actual, final Description description) {
                describer(expected, actual, ignoreNames, description);
            }
        };
    }

    static boolean compare(final Object expected, final Object actual) {
        return compare(expected, actual, Collections.emptyList());
    }

    static boolean compare(final Object expected, final Object actual, final List<String> ignoreNames) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }
        if (!actual.getClass().getName().equals(expected.getClass().getName())) {
            return false;
        }
        List<Field> fields = getFields(actual.getClass());
        for (Field field : fields) {
            if (ignoreNames.contains(field.getName())) {
                continue;
            }
            field.setAccessible(true);
            Object actualPropertyValue;
            Object expectedPropertyValue;
            try {
                actualPropertyValue = field.get(actual);
                expectedPropertyValue = field.get(expected);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            if (nominallyTheSame(field.getName())) {
                if (actualPropertyValue == null || expectedPropertyValue == null) {
                    if (actualPropertyValue != expectedPropertyValue) {
                        return false;
                    }
                } else if (!actualPropertyValue.getClass().equals(expectedPropertyValue.getClass())) {
                    return false;
                }
            } else if (!objectsEqual(actualPropertyValue, expectedPropertyValue)) {
                if ((actualPropertyValue == null || expectedPropertyValue == null)) {
                    List<Object> pair = Arrays.asList(actualPropertyValue, expectedPropertyValue);
                    if (!pair.equals(NULL_LIST)) {
                        return false;
                    }
                } else if (actualPropertyValue instanceof List && expectedPropertyValue instanceof List) {
                    List<?> actualList = (List<?>) actualPropertyValue;
                    List<?> expectedList = (List<?>) expectedPropertyValue;
                    if (actualList.size() == expectedList.size()) {
                        for (int i = 0; i < actualList.size(); i++) {
                            if (!compare(actualList.get(i), expectedList.get(i))) {
                                return false;
                            }
                        }
                    } else {
                        return false;
                    }
                } else if (actualPropertyValue.getClass().getName().startsWith("com.mongodb")
                        && actualPropertyValue.getClass().equals(expectedPropertyValue.getClass())) {
                    if (!compare(actualPropertyValue, expectedPropertyValue)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    static void describer(final Object expected, final Object actual, final Description description) {
        describer(expected, actual, Collections.emptyList(), description);
    }

    static void describer(final Object expected, final Object actual, final List<String> ignoreNames,
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

        List<Field> fields = getFields(actual.getClass());
        for (Field field : fields) {
            if (ignoreNames.contains(field.getName())) {
                continue;
            }
            field.setAccessible(true);
            Object actualPropertyValue;
            Object expectedPropertyValue;
            try {
                actualPropertyValue = field.get(actual);
                expectedPropertyValue = field.get(expected);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            if (nominallyTheSame(field.getName())) {
                if (actualPropertyValue != null && expectedPropertyValue != null
                        && !actualPropertyValue.getClass().equals(expectedPropertyValue.getClass())) {
                    description.appendText("different classes in " + field.getName() + " : "
                            + expectedPropertyValue.getClass().getName() + " != "
                            + actualPropertyValue.getClass().getName() + ", ");
                }
            } else if (!objectsEqual(actualPropertyValue, expectedPropertyValue)) {
                if ((actualPropertyValue == null || expectedPropertyValue == null
                        || actualPropertyValue.getClass() == null || expectedPropertyValue.getClass() == null)) {
                    List<Object> pair = Arrays.asList(actualPropertyValue, expectedPropertyValue);
                    if (!pair.equals(NULL_LIST)) {
                        description.appendText("different values in " + field.getName() + " : "
                                + expectedPropertyValue + " != " + actualPropertyValue + "\n");
                    }
                } else if (actualPropertyValue instanceof List && expectedPropertyValue instanceof List) {
                    List<?> actualList = (List<?>) actualPropertyValue;
                    List<?> expectedList = (List<?>) expectedPropertyValue;
                    if (actualList.size() == expectedList.size()) {
                        for (int i = 0; i < actualList.size(); i++) {
                            if (!compare(actualList.get(i), expectedList.get(i))) {
                                describer(actualList.get(i), expectedList.get(i), description);
                            }
                        }
                    }
                } else if (actualPropertyValue.getClass().getName().startsWith("com.mongodb")
                        && actualPropertyValue.getClass().equals(expectedPropertyValue.getClass())) {
                    describer(actualPropertyValue, expectedPropertyValue, description);
                }
                description.appendText("different values in " + field.getName() + " : "
                        + expectedPropertyValue + " != " + actualPropertyValue + "\n");
            }
        }
    }

    static List<Field> getFields(final Class<?> curClass) {
        if (curClass == Object.class) {
            return new ArrayList<>();
        }
        List<Field> fields = getFields(curClass.getSuperclass());
        for (Field field : curClass.getDeclaredFields()) {
            if (!field.isSynthetic() && !Modifier.isStatic(field.getModifiers()) && !field.getName().contains("$")) {
                fields.add(field);
            }
        }
        return fields;
    }

    static boolean nominallyTheSame(final String propertyName) {
        return "decoder".equals(propertyName) || "executor".equals(propertyName);
    }

    private static boolean objectsEqual(final Object a, final Object b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        return a.equals(b);
    }

    private CustomMatchers() {
    }
}
