/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.connector.flink;

import org.apache.flink.util.TestLogger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class OceanBaseOracleTestBase extends TestLogger {
    public static final String URL = System.getenv("URL");
    public static final String USER_NAME = System.getenv("USER_NAME");
    public static final String PASSWORD = System.getenv("PASSWORD");
    public static final String SCHEMA_NAME = System.getenv("SCHEMA_NAME");
    public static final String HOST = System.getenv("HOST");
    public static final String PORT = System.getenv("PORT");

    public static final String DRIVER_CLASS_NAME = "com.oceanbase.jdbc.Driver";

    protected String getUrl() {
        return URL;
    }

    protected abstract String getTestTable();

    protected String getUsername() {
        return USER_NAME;
    }

    protected String getPassword() {
        return PASSWORD;
    }

    protected String getDatabaseName() {
        return SCHEMA_NAME;
    }

    protected Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("url", getUrl());
        options.put("username", getUsername());
        options.put("password", getPassword());
        options.put("schema-name", getDatabaseName());
        options.put("table-name", getTestTable());
        options.put("driver-class-name", DRIVER_CLASS_NAME);
        return options;
    }

    protected String getOptionsString() {
        return getOptions().entrySet().stream()
                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
