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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.net.URISyntaxException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

public class OBKVHBaseConnectorByODPITCase extends OceanBaseMySQLTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseConnectorByODPITCase.class);
    private static final String CLUSTER_NAME = "flink-oceanbase-ci";
    private static final String APP_NAME = "odp_test";
    private static final int ODP_PORT = 2883;
    private static final int RPC_PORT = 2885;
    private static final String ODP_USERNAME = "root@sys";
    private static final String PROXY_USERNAME = "proxyro@sys";
    private static final String ODP_PASSWORD = "123456";
    private GenericContainer<?> obproxy;
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public Map<String, Object> getODPOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("url", getODPUrl());
        options.put("odp-mode", true);
        options.put("username", ODP_USERNAME);
        options.put("password", ODP_PASSWORD);
        options.put("sys.username", getUsername());
        options.put("sys.password", getSysPassword());
        options.put("schema-name", getSchemaName());
        return options;
    }

    GenericContainer<?> initObProxyContainer(String rsList) {
        return new GenericContainer<>("oceanbase/obproxy-ce:4.3.1.0-4")
                .withNetwork(NETWORK)
                .withEnv("PROXYRO_PASSWORD", ODP_PASSWORD)
                .withEnv("OB_CLUSTER", CLUSTER_NAME)
                .withEnv("APP_NAME", APP_NAME)
                .withEnv("RS_LIST", rsList)
                .withExposedPorts(ODP_PORT, RPC_PORT)
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    @Before
    public void before() throws SQLException {
        // Create a Proxyro account
        createProxyUser();
        // test user create is successfully
        testConnection(getJdbcUrl(), PROXY_USERNAME, ODP_PASSWORD);
        // Make sure that the OceanBase container is started and the IP address and port address are
        String rsList = getRsList();
        LOG.info("OceanBase's network IP and port: {}", rsList);
        obproxy = initObProxyContainer(rsList);
        Startables.deepStart(Stream.of(obproxy)).join();
        given().ignoreExceptions()
                .await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> testConnection(getOdpJdbcUrl(), ODP_USERNAME, ODP_PASSWORD));
        createTable(getOdpJdbcUrl(), ODP_USERNAME, ODP_PASSWORD, "sql/odphtable.sql");
        CheckTableExists(getOdpJdbcUrl(), ODP_USERNAME, ODP_PASSWORD);
    }

    @AfterEach
    public void tearDown() {
        if (obproxy != null) {
            obproxy.close();
        }
    }

    @Test
    public void testOdpMode() throws Exception {
        // test ODP MODE
        String optionsString = getODPOptions(getODPOptions());
        LOG.info("The test parameters of testOdpModeSink are:{}", optionsString);
        sinkTest(optionsString);
    }

    protected List<String> queryHTable(String tableName, RowConverter rowConverter)
            throws SQLException {
        return queryTable(tableName, Arrays.asList("K", "Q", "V"), rowConverter);
    }

    protected String integer(Integer n) {
        if (n == null) {
            return "CAST(NULL AS INT)";
        }
        return n.toString();
    }

    protected String string(String s) {
        if (s == null) {
            return "CAST(NULL AS STRING)";
        }
        return "'" + s + "'";
    }

    public void sinkTest(String getOptionsString) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " rowkey STRING,"
                        + " family1 ROW<q1 INT>,"
                        + " family2 ROW<q2 STRING, q3 INT>,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='obkv-hbase',"
                        + "  'table-name'='odphtable',"
                        + getOptionsString
                        + ");");

        String insertSql =
                String.format(
                        "INSERT INTO target VALUES "
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s))",
                        string("1"),
                        integer(1),
                        string("1"),
                        integer(1),
                        string("2"),
                        integer(null),
                        string("2"),
                        integer(null),
                        string("3"),
                        integer(3),
                        string(null),
                        integer(null),
                        string("4"),
                        integer(4),
                        string("4"),
                        integer(null));

        tEnv.executeSql(insertSql).await();

        List<String> expected1 = Arrays.asList("1,q1,1", "3,q1,3", "4,q1,4");
        List<String> expected2 = Arrays.asList("1,q2,1", "1,q3,1", "2,q2,2", "4,q2,4");

        RowConverter rowConverter =
                (rs, columnCount) -> {
                    String k = Bytes.toString(rs.getBytes("K"));
                    String q = Bytes.toString(rs.getBytes("Q"));
                    byte[] bytes = rs.getBytes("V");
                    String v;
                    switch (q) {
                        case "q1":
                        case "q3":
                            v = String.valueOf(Bytes.toInt(bytes));
                            break;
                        case "q2":
                            v = Bytes.toString(bytes);
                            break;
                        default:
                            throw new RuntimeException("Unknown qualifier: " + q);
                    }
                    return k + "," + q + "," + v;
                };

        waitingAndAssertTableCount("odphtable$family1", expected1.size());
        waitingAndAssertTableCount("odphtable$family2", expected2.size());

        List<String> actual1 = queryHTable("odphtable$family1", rowConverter);
        assertEqualsInAnyOrder(expected1, actual1);

        List<String> actual2 = queryHTable("odphtable$family2", rowConverter);
        assertEqualsInAnyOrder(expected2, actual2);

        dropTables("odphtable$family1", "odphtable$family2");
    }

    public String getODPUrl() {
        String odpUrl = obproxy.getHost() + ":" + obproxy.getMappedPort(RPC_PORT);
        LOG.info("odp url value is: {}", odpUrl);
        return odpUrl;
    }

    public String getOdpJdbcUrl() {
        return "jdbc:mysql://"
                + obproxy.getHost()
                + ":"
                + obproxy.getMappedPort(ODP_PORT)
                + "/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    }

    public void testConnection(String jdbcUrl, String userName, String password) {
        try {
            LOG.info(
                    "odpJDBC URL and username and password is:{},{},{}",
                    jdbcUrl,
                    userName,
                    password);
            Connection connection = DriverManager.getConnection(jdbcUrl, userName, password);
            Statement statement = connection.createStatement();
            String sql = "SELECT VERSION();";
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                LOG.info("VERSION is:{}", rs.getString("VERSION()"));
            }
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable(String jdbcUrl, String userName, String password, String sqlFile) {
        try {
            final URL file = getClass().getClassLoader().getResource(sqlFile);
            assertNotNull("Cannot locate " + sqlFile, file);
            Connection connection = DriverManager.getConnection(jdbcUrl, userName, password);
            Statement statement = connection.createStatement();
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
            connection.close();
        } catch (SQLException | URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void CheckTableExists(String jdbcUrl, String userName, String password) {
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, userName, password);
            Statement statement = connection.createStatement();
            String sql =
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"
                            + getSchemaName()
                            + "'";
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                LOG.info("TABLE_NAME is:{}", rs.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
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
        try (Connection connection =
                        DriverManager.getConnection(getOdpJdbcUrl(), ODP_USERNAME, ODP_PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT COUNT(1) FROM " + tableName);
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public void dropTables(String... tableNames) throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(getOdpJdbcUrl(), ODP_USERNAME, ODP_PASSWORD);
                Statement statement = connection.createStatement()) {
            for (String tableName : tableNames) {
                statement.execute("DROP TABLE " + tableName);
            }
        }
    }

    public List<String> queryTable(String tableName, List<String> fields, RowConverter rowConverter)
            throws SQLException {
        String sql = String.format("SELECT %s FROM %s", String.join(", ", fields), tableName);
        List<String> result = new ArrayList<>();

        try (Connection connection =
                        DriverManager.getConnection(getOdpJdbcUrl(), ODP_USERNAME, ODP_PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()) {
                result.add(rowConverter.convert(rs, metaData.getColumnCount()));
            }
        }
        return result;
    }
}
