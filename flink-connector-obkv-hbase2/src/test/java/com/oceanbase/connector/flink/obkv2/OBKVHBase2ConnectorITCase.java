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

package com.oceanbase.connector.flink.obkv2;

import com.oceanbase.connector.flink.OceanBaseMySQLTestBase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Integration tests for {@link OBKVHBase2DynamicTableFactory}. */
public class OBKVHBase2ConnectorITCase extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBase2ConnectorITCase.class);

    @BeforeAll
    public static void setup() throws Exception {
        CONFIG_SERVER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();

        CONTAINER
                .withEnv("OB_CONFIGSERVER_ADDRESS", getConfigServerAddress())
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .start();

        String password = "test";
        createSysUser("proxyro", password);

        ODP.withPassword(password)
                .withConfigUrl(getConfigUrlForODP())
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .start();
    }

    @AfterAll
    public static void tearDown() {
        Stream.of(CONFIG_SERVER, CONTAINER, ODP).forEach(GenericContainer::stop);
    }

    @BeforeEach
    public void before() throws Exception {
        initialize("sql/htable2.sql");
    }

    @AfterEach
    public void after() throws Exception {
        dropTables("htable2$f");
    }

    @Override
    public Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("username", getUsername() + "#" + getClusterName());
        options.put("password", getPassword());
        options.put("schema-name", getSchemaName());
        return options;
    }

    @Test
    public void testSinkBasic() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());

        testBasicSink(options);
    }

    @Test
    public void testSinkWithODP() throws Exception {
        Map<String, String> options = getOptions();
        options.put("odp-mode", "true");
        options.put("odp-ip", ODP.getHost());
        options.put("odp-port", String.valueOf(ODP.getRpcPort()));

        testBasicSink(options);
    }

    @Test
    public void testSinkWithIgnoreNull() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());
        options.put("ignoreNullWhenUpdate", "true");

        testIgnoreNullSink(options);
    }

    @Test
    public void testSinkWithExcludeColumns() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());
        options.put("excludeUpdateColumns", "col3");

        testExcludeColumnsSink(options);
    }

    @Test
    public void testSinkWithDynamicColumn() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());
        options.put("dynamicColumnSink", "true");

        testDynamicColumnSink(options);
    }

    @Test
    public void testSinkWithTsColumn() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());

        testTsColumnSink(options);
    }

    @Test
    public void testSinkWithTsMap() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());

        testTsMapSink(options);
    }

    private void testBasicSink(Map<String, String> options) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        // Create table using flat structure (new obkv-hbase2 style)
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " rowkey STRING,"
                        + " col1 INT,"
                        + " col2 STRING,"
                        + " col3 INT,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='obkv-hbase2',"
                        + "  'table-name'='htable2',"
                        + "  'columnFamily'='f',"
                        + getOptionsString(options)
                        + ");");

        String insertSql =
                String.format(
                        "INSERT INTO target VALUES "
                                + "(%s, %s, %s, %s), "
                                + "(%s, %s, %s, %s), "
                                + "(%s, %s, %s, %s)",
                        string("row1"),
                        integer(100),
                        string("value1"),
                        integer(1),
                        string("row2"),
                        integer(200),
                        string("value2"),
                        integer(2),
                        string("row3"),
                        integer(300),
                        string("value3"),
                        integer(3));

        tEnv.executeSql(insertSql).await();

        // Expected results: 3 rows * 3 columns = 9 cell values
        List<String> expected =
                Arrays.asList(
                        "row1,col1,100",
                        "row1,col2,value1",
                        "row1,col3,1",
                        "row2,col1,200",
                        "row2,col2,value2",
                        "row2,col3,2",
                        "row3,col1,300",
                        "row3,col2,value3",
                        "row3,col3,3");

        RowConverter rowConverter =
                (rs, columnCount) -> {
                    String k = Bytes.toString(rs.getBytes("K"));
                    String q = Bytes.toString(rs.getBytes("Q"));
                    byte[] bytes = rs.getBytes("V");
                    String v;
                    if (q.equals("col2")) {
                        v = Bytes.toString(bytes);
                    } else {
                        v = String.valueOf(Bytes.toInt(bytes));
                    }
                    return k + "," + q + "," + v;
                };

        waitingAndAssertTableCount("htable2$f", expected.size());

        List<String> actual = queryHTable("htable2$f", rowConverter);
        assertEqualsInAnyOrder(expected, actual);
    }

    private void testIgnoreNullSink(Map<String, String> options) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " rowkey STRING,"
                        + " col1 INT,"
                        + " col2 STRING,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='obkv-hbase2',"
                        + "  'table-name'='htable2',"
                        + "  'columnFamily'='f',"
                        + getOptionsString(options)
                        + ");");

        String insertSql =
                String.format(
                        "INSERT INTO target VALUES " + "(%s, %s, %s), " + "(%s, %s, %s)",
                        string("row1"),
                        integer(100),
                        string("value1"),
                        string("row2"),
                        integer(null), // null value should be ignored
                        string("value2"));

        tEnv.executeSql(insertSql).await();

        // Expected: row1 has both columns, row2 only has col2 (col1 is null and ignored)
        List<String> expected =
                Arrays.asList("row1,col1,100", "row1,col2,value1", "row2,col2,value2");

        RowConverter rowConverter =
                (rs, columnCount) -> {
                    String k = Bytes.toString(rs.getBytes("K"));
                    String q = Bytes.toString(rs.getBytes("Q"));
                    byte[] bytes = rs.getBytes("V");
                    String v =
                            q.equals("col2")
                                    ? Bytes.toString(bytes)
                                    : String.valueOf(Bytes.toInt(bytes));
                    return k + "," + q + "," + v;
                };

        waitingAndAssertTableCount("htable2$f", expected.size());

        List<String> actual = queryHTable("htable2$f", rowConverter);
        assertEqualsInAnyOrder(expected, actual);
    }

    private void testExcludeColumnsSink(Map<String, String> options) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " rowkey STRING,"
                        + " col1 INT,"
                        + " col2 STRING,"
                        + " col3 INT,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='obkv-hbase2',"
                        + "  'table-name'='htable2',"
                        + "  'columnFamily'='f',"
                        + getOptionsString(options)
                        + ");");

        String insertSql =
                String.format(
                        "INSERT INTO target VALUES (%s, %s, %s, %s)",
                        string("row1"), integer(100), string("value1"), integer(999));

        tEnv.executeSql(insertSql).await();

        // Expected: col3 should be excluded, only col1 and col2 are written
        List<String> expected = Arrays.asList("row1,col1,100", "row1,col2,value1");

        RowConverter rowConverter =
                (rs, columnCount) -> {
                    String k = Bytes.toString(rs.getBytes("K"));
                    String q = Bytes.toString(rs.getBytes("Q"));
                    byte[] bytes = rs.getBytes("V");
                    String v =
                            q.equals("col2")
                                    ? Bytes.toString(bytes)
                                    : String.valueOf(Bytes.toInt(bytes));
                    return k + "," + q + "," + v;
                };

        waitingAndAssertTableCount("htable2$f", expected.size());

        List<String> actual = queryHTable("htable2$f", rowConverter);
        assertEqualsInAnyOrder(expected, actual);
    }

    private void testDynamicColumnSink(Map<String, String> options) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " rowkey STRING,"
                        + " columnKey STRING,"
                        + " columnValue STRING,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='obkv-hbase2',"
                        + "  'table-name'='htable2',"
                        + "  'columnFamily'='f',"
                        + getOptionsString(options)
                        + ");");

        String insertSql =
                String.format(
                        "INSERT INTO target VALUES "
                                + "(%s, %s, %s), "
                                + "(%s, %s, %s), "
                                + "(%s, %s, %s)",
                        string("row1"),
                        string("dynamic_col1"),
                        string("value1"),
                        string("row1"),
                        string("dynamic_col2"),
                        string("value2"),
                        string("row2"),
                        string("dynamic_col3"),
                        string("value3"));

        tEnv.executeSql(insertSql).await();

        // Expected: dynamic columns
        List<String> expected =
                Arrays.asList(
                        "row1,dynamic_col1,value1",
                        "row1,dynamic_col2,value2",
                        "row2,dynamic_col3,value3");

        RowConverter rowConverter =
                (rs, columnCount) -> {
                    String k = Bytes.toString(rs.getBytes("K"));
                    String q = Bytes.toString(rs.getBytes("Q"));
                    String v = Bytes.toString(rs.getBytes("V"));
                    return k + "," + q + "," + v;
                };

        waitingAndAssertTableCount("htable2$f", expected.size());

        List<String> actual = queryHTable("htable2$f", rowConverter);
        assertEqualsInAnyOrder(expected, actual);
    }

    private void testTsColumnSink(Map<String, String> options) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        // Create table with tsColumn configuration
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " rowkey STRING,"
                        + " ts_col BIGINT,"
                        + " col1 INT,"
                        + " col2 STRING,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='obkv-hbase2',"
                        + "  'table-name'='htable2',"
                        + "  'columnFamily'='f',"
                        + "  'tsColumn'='ts_col',"
                        + "  'tsInMills'='true',"
                        + getOptionsString(options)
                        + ");");

        long currentTime = System.currentTimeMillis();
        String insertSql =
                String.format(
                        "INSERT INTO target VALUES " + "(%s, %s, %s, %s), " + "(%s, %s, %s, %s)",
                        string("row1"),
                        currentTime,
                        integer(100),
                        string("value1"),
                        string("row2"),
                        currentTime + 1000,
                        integer(200),
                        string("value2"));

        tEnv.executeSql(insertSql).await();

        // Expected: ts_col should be automatically excluded, only col1 and col2 are written
        List<String> expected =
                Arrays.asList(
                        "row1,col1,100", "row1,col2,value1", "row2,col1,200", "row2,col2,value2");

        RowConverter rowConverter =
                (rs, columnCount) -> {
                    String k = Bytes.toString(rs.getBytes("K"));
                    String q = Bytes.toString(rs.getBytes("Q"));
                    byte[] bytes = rs.getBytes("V");
                    String v =
                            q.equals("col2")
                                    ? Bytes.toString(bytes)
                                    : String.valueOf(Bytes.toInt(bytes));
                    return k + "," + q + "," + v;
                };

        waitingAndAssertTableCount("htable2$f", expected.size());

        List<String> actual = queryHTable("htable2$f", rowConverter);
        assertEqualsInAnyOrder(expected, actual);
    }

    private void testTsMapSink(Map<String, String> options) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        // Create table with tsMap configuration
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " rowkey STRING,"
                        + " ts_col1 BIGINT,"
                        + " ts_col2 BIGINT,"
                        + " col1 INT,"
                        + " col2 STRING,"
                        + " col3 INT,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='obkv-hbase2',"
                        + "  'table-name'='htable2',"
                        + "  'columnFamily'='f',"
                        + "  'tsMap'='ts_col1:col1;ts_col1:col2;ts_col2:col3',"
                        + "  'tsInMills'='true',"
                        + getOptionsString(options)
                        + ");");

        long currentTime = System.currentTimeMillis();
        String insertSql =
                String.format(
                        "INSERT INTO target VALUES (%s, %s, %s, %s, %s, %s)",
                        string("row1"),
                        currentTime,
                        currentTime + 1000,
                        integer(100),
                        string("value1"),
                        integer(1));

        tEnv.executeSql(insertSql).await();

        // Expected: ts_col1 and ts_col2 should be automatically excluded,
        // only col1, col2, and col3 are written
        List<String> expected = Arrays.asList("row1,col1,100", "row1,col2,value1", "row1,col3,1");

        RowConverter rowConverter =
                (rs, columnCount) -> {
                    String k = Bytes.toString(rs.getBytes("K"));
                    String q = Bytes.toString(rs.getBytes("Q"));
                    byte[] bytes = rs.getBytes("V");
                    String v =
                            q.equals("col2")
                                    ? Bytes.toString(bytes)
                                    : String.valueOf(Bytes.toInt(bytes));
                    return k + "," + q + "," + v;
                };

        waitingAndAssertTableCount("htable2$f", expected.size());

        List<String> actual = queryHTable("htable2$f", rowConverter);
        assertEqualsInAnyOrder(expected, actual);
    }

    protected List<String> queryHTable(String tableName, RowConverter rowConverter)
            throws SQLException {
        return queryTable(tableName, Arrays.asList("K", "Q", "V"), rowConverter);
    }
}
