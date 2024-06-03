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

import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionProvider;
import com.oceanbase.connector.flink.sink.OBKVHBaseRecordFlusher;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.HTableInfo;
import com.oceanbase.connector.flink.table.OBKVHBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class OBKVHBaseConnectorITCase extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseConnectorITCase.class);

    @ClassRule public static final GenericContainer<?> CONTAINER = container("sql/init.sql");

    private static final String TEST_TABLE = "htable";

    protected String getConfigUrl() {
        try (Connection connection =
                        DriverManager.getConnection(
                                getJdbcUrl(CONTAINER), SYS_USERNAME, SYS_PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SHOW PARAMETERS LIKE 'obconfig_url'");
            String configUrl = rs.next() ? rs.getString("VALUE") : null;
            if (configUrl == null || configUrl.isEmpty()) {
                throw new RuntimeException("obconfig_url not found");
            }
            LOG.info("Got obconfig_url: {}", configUrl);
            return configUrl;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("url", getConfigUrl());
        options.put("sys.username", SYS_USERNAME);
        options.put("sys.password", SYS_PASSWORD);
        options.put("username", TEST_USERNAME + "#" + CLUSTER_NAME);
        options.put("password", TEST_PASSWORD);
        options.put("schema-name", TEST_DATABASE);
        options.put("table-name", TEST_TABLE);
        return options;
    }

    private HTableInterface client;

    @Before
    public void before() throws Exception {
        OBKVHBaseConnectorOptions options = new OBKVHBaseConnectorOptions(getOptions());
        OBKVHBaseConnectionProvider connectionProvider = new OBKVHBaseConnectionProvider(options);
        TableId tableId = new TableId(options.getSchemaName(), options.getTableName());
        client = connectionProvider.getHTableClient(tableId);
    }

    @After
    public void after() throws Exception {
        if (client == null) {
            return;
        }
        client.delete(
                Arrays.asList(
                        deleteFamily("1", "family1"),
                        deleteFamily("1", "family2"),
                        deleteFamily("2", "family2"),
                        deleteFamily("3", "family1"),
                        deleteFamily("4", "family1"),
                        deleteFamily("4", "family2")));
        client.close();
        client = null;
    }

    private Delete deleteFamily(String rowKey, String family) {
        return new Delete(Bytes.toBytes(rowKey)).deleteFamily(Bytes.toBytes(family));
    }

    @Test
    public void testDataStreamSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OBKVHBaseConnectorOptions connectorOptions = new OBKVHBaseConnectorOptions(getOptions());
        ResolvedSchema physicalSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("rowkey", DataTypes.STRING().notNull()),
                                Column.physical(
                                        "family1",
                                        DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "q1", DataTypes.INT().nullable()))
                                                .notNull()),
                                Column.physical(
                                        "family2",
                                        DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "q2",
                                                                DataTypes.STRING().nullable()),
                                                        DataTypes.FIELD(
                                                                "q3", DataTypes.INT().nullable()))
                                                .notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("rowkey")));
        OceanBaseSink<RowData> sink =
                new OceanBaseSink<>(
                        connectorOptions,
                        null,
                        new OBKVHBaseRowDataSerializationSchema(
                                new HTableInfo(
                                        new TableId(
                                                connectorOptions.getSchemaName(),
                                                connectorOptions.getTableName()),
                                        physicalSchema)),
                        DataChangeRecord.KeyExtractor.simple(),
                        new OBKVHBaseRecordFlusher(connectorOptions));

        List<RowData> dataSet =
                Arrays.asList(
                        rowData("1", 1, "1", 1),
                        rowData("2", null, "2", null),
                        rowData("3", 3, null, null),
                        rowData("4", 4, "4", null));

        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();

        validateSinkResults();
    }

    private RowData rowData(String rowKey, Integer q1, String q2, Integer q3) {
        return GenericRowData.of(
                StringData.fromString(rowKey),
                GenericRowData.of(q1),
                GenericRowData.of(StringData.fromString(q2), q3));
    }

    @Test
    public void testSink() throws Exception {
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
                        + getOptionsString()
                        + ");");

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO target VALUES %s, %s, %s, %s",
                                row("1", 1, "1", 1),
                                row("2", null, "2", null),
                                row("3", 3, null, null),
                                row("4", 4, "4", null)))
                .await();

        validateSinkResults();
    }

    private void validateSinkResults() throws Exception {
        assertEqualsInAnyOrder(
                Collections.singletonList("1,q1,1"), queryHTable(client, "family1", "1"));
        assertTrue(queryHTable(client, "family1", "2").isEmpty());
        assertEqualsInAnyOrder(
                Collections.singletonList("3,q1,3"), queryHTable(client, "family1", "3"));
        assertEqualsInAnyOrder(
                Collections.singletonList("4,q1,4"), queryHTable(client, "family1", "4"));

        assertEqualsInAnyOrder(
                Arrays.asList("1,q2,1", "1,q3,1"), queryHTable(client, "family2", "1"));
        assertEqualsInAnyOrder(
                Collections.singletonList("2,q2,2"), queryHTable(client, "family2", "2"));
        assertTrue(queryHTable(client, "family2", "3").isEmpty());
        assertEqualsInAnyOrder(
                Collections.singletonList("4,q2,4"), queryHTable(client, "family2", "4"));
    }

    private List<String> queryHTable(HTableInterface client, String family, String rowKey)
            throws IOException {
        List<String> result = new ArrayList<>();
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(family));
        Result r = client.get(get);
        if (r == null || r.isEmpty()) {
            return result;
        }
        for (KeyValue kv : r.list()) {
            String column = Bytes.toString(kv.getQualifier());
            result.add(
                    String.format(
                            "%s,%s,%s",
                            rowKey,
                            column,
                            "q2".equals(column)
                                    ? Bytes.toString(kv.getValue())
                                    : String.valueOf(Bytes.toInt(kv.getValue()))));
        }
        return result;
    }

    private String row(String key, Integer q1, String q2, Integer q3) {
        return String.format(
                "(%s, ROW(%s), ROW(%s, %s))", string(key), integer(q1), string(q2), integer(q3));
    }

    private String integer(Integer n) {
        if (n == null) {
            return "CAST(NULL AS INT)";
        }
        return n.toString();
    }

    private String string(String s) {
        if (s == null) {
            return "CAST(NULL AS STRING)";
        }
        return "'" + s + "'";
    }
}
