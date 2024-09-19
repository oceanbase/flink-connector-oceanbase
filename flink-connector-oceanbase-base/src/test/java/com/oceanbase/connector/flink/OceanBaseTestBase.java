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

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class OceanBaseTestBase implements OceanBaseMetadata {

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public static String getOptionsString(Map<String, String> options) {
        assertNotNull(options);
        return options.entrySet().stream()
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

    public Map<String, String> getBaseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("url", getJdbcUrl());
        options.put("username", getUsername());
        options.put("password", getPassword());
        return options;
    }

    public Map<String, String> getOptions() {
        Map<String, String> options = getBaseOptions();
        options.put("schema-name", getSchemaName());
        return options;
    }

    public String getOptionsString() {
        return getOptionsString(getOptions());
    }

    public Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
    }

    public Connection getSysJdbcConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), getSysUsername(), getSysPassword());
    }

    public String getSysParameter(String parameter) {
        try (Connection connection = getSysJdbcConnection();
                Statement statement = connection.createStatement()) {
            String sql = String.format("SHOW PARAMETERS LIKE '%s'", parameter);
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                return rs.getString("VALUE");
            }
            throw new RuntimeException("Parameter '" + parameter + "' not found");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createSysUser(String user, String password) throws SQLException {
        assert user != null && password != null;
        try (Connection connection = getSysJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE USER '" + user + "' IDENTIFIED BY '" + password + "'");
            statement.execute("GRANT ALL PRIVILEGES ON *.* TO '" + user + "'@'%'");
        }
    }

    public void initialize(String sqlFile) {
        final URL file = getClass().getClassLoader().getResource(sqlFile);
        assertNotNull("Cannot locate " + sqlFile, file);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(file.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void waitingAndAssertTableCount(String tableName, int expectedCount)
            throws InterruptedException {
        int tableRowsCount = 0;
        for (int i = 0; i < 100; ++i) {
            try {
                tableRowsCount = getTableRowsCount(tableName);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to get table rows count for table " + tableName, e);
            }

            if (tableRowsCount < expectedCount) {
                Thread.sleep(100);
            }
        }
        assertEquals(expectedCount, tableRowsCount);
    }

    public int getTableRowsCount(String tableName) throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT COUNT(1) FROM " + tableName);
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public List<String> queryTable(String tableName) throws SQLException {
        return queryTable(tableName, Collections.singletonList("*"));
    }

    public List<String> queryTable(String tableName, List<String> fields) throws SQLException {
        return queryTable(tableName, fields, this::getRowString);
    }

    public List<String> queryTable(String tableName, List<String> fields, RowConverter rowConverter)
            throws SQLException {
        String sql = String.format("SELECT %s FROM %s", String.join(", ", fields), tableName);
        List<String> result = new ArrayList<>();

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()) {
                result.add(rowConverter.convert(rs, metaData.getColumnCount()));
            }
        }
        return result;
    }

    protected String getRowString(ResultSet rs, int columnCount) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(rs.getObject(i + 1));
        }
        return sb.toString();
    }

    public void dropTables(String... tableNames) throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            for (String tableName : tableNames) {
                statement.execute("DROP TABLE " + tableName);
            }
        }
    }

    @FunctionalInterface
    public interface RowConverter {
        String convert(ResultSet rs, int columnCount) throws SQLException;
    }
}
