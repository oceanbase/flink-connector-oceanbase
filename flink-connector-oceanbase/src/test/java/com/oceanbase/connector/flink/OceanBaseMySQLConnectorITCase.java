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

import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.sink.OceanBaseRecordFlusher;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.OceanBaseTestData;
import com.oceanbase.connector.flink.table.OceanBaseTestDataSerializationSchema;
import com.oceanbase.connector.flink.table.SchemaChangeRecord;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

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
import org.apache.flink.types.RowKind;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OceanBaseMySQLConnectorITCase extends OceanBaseMySQLTestBase {

    @Test
    public void testSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        initialize("sql/mysql/products.sql");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'table-name'='products',"
                        + getOptionsString()
                        + ");");

        tEnv.executeSql(
                        "INSERT INTO target "
                                + "VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14),"
                                + "       (102, 'car battery', '12V car battery', 8.1),"
                                + "       (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),"
                                + "       (104, 'hammer', '12oz carpenter''s hammer', 0.75),"
                                + "       (105, 'hammer', '14oz carpenter''s hammer', 0.875),"
                                + "       (106, 'hammer', '16oz carpenter''s hammer', 1.0),"
                                + "       (107, 'rocks', 'box of assorted rocks', 5.3),"
                                + "       (108, 'jacket', 'water resistent black wind breaker', 0.1),"
                                + "       (109, 'spare tire', '24 inch spare tire', 22.2);")
                .await();

        List<String> expected =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.1400000000",
                        "102,car battery,12V car battery,8.1000000000",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
                        "104,hammer,12oz carpenter's hammer,0.7500000000",
                        "105,hammer,14oz carpenter's hammer,0.8750000000",
                        "106,hammer,16oz carpenter's hammer,1.0000000000",
                        "107,rocks,box of assorted rocks,5.3000000000",
                        "108,jacket,water resistent black wind breaker,0.1000000000",
                        "109,spare tire,24 inch spare tire,22.2000000000");

        waitingAndAssertTableCount("products", expected.size());

        List<String> actual = queryTable("products");

        assertEqualsInAnyOrder(expected, actual);

        dropTables("products");
    }

    @Test
    public void testDirectLoadSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        initialize("sql/mysql/products.sql");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'direct-load.enabled'='true',"
                        + "  'direct-load.host'='"
                        + getHost()
                        + "',"
                        + "  'direct-load.port'='"
                        + getRpcPort()
                        + "',"
                        + "  'table-name'='products',"
                        + getOptionsString()
                        + ");");

        tEnv.executeSql(
                        "INSERT INTO target "
                                + "VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14),"
                                + "       (102, 'car battery', '12V car battery', 8.1),"
                                + "       (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),"
                                + "       (104, 'hammer', '12oz carpenter''s hammer', 0.75),"
                                + "       (105, 'hammer', '14oz carpenter''s hammer', 0.875),"
                                + "       (106, 'hammer', '16oz carpenter''s hammer', 1.0),"
                                + "       (107, 'rocks', 'box of assorted rocks', 5.3),"
                                + "       (108, 'jacket', 'water resistent black wind breaker', 0.1),"
                                + "       (109, 'spare tire', '24 inch spare tire', 22.2);")
                .await();

        List<String> expected =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.1400000000",
                        "102,car battery,12V car battery,8.1000000000",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
                        "104,hammer,12oz carpenter's hammer,0.7500000000",
                        "105,hammer,14oz carpenter's hammer,0.8750000000",
                        "106,hammer,16oz carpenter's hammer,1.0000000000",
                        "107,rocks,box of assorted rocks,5.3000000000",
                        "108,jacket,water resistent black wind breaker,0.1000000000",
                        "109,spare tire,24 inch spare tire,22.2000000000");

        waitingAndAssertTableCount("products", expected.size());

        List<String> actual = queryTable("products");

        assertEqualsInAnyOrder(expected, actual);

        dropTables("products");
    }

    @Test
    public void testGis() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        initialize("sql/mysql/gis_types.sql");

        Map<String, String> options = getOptions();
        options.put("table-name", "gis_types");

        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(options);
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        OceanBaseSink<RowData> sink =
                new OceanBaseSink<>(
                        connectorOptions,
                        null,
                        new OceanBaseRowDataSerializationSchema(
                                new TableInfo(
                                        new TableId(
                                                connectionProvider.getDialect()::getFullTableName,
                                                connectorOptions.getSchemaName(),
                                                connectorOptions.getTableName()),
                                        Collections.singletonList("id"),
                                        Arrays.asList(
                                                "id",
                                                "point_c",
                                                "geometry_c",
                                                "linestring_c",
                                                "polygon_c",
                                                "multipoint_c",
                                                "multiline_c",
                                                "multipolygon_c",
                                                "geometrycollection_c"),
                                        Arrays.asList(
                                                DataTypes.INT().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType()),
                                        x -> "id".equals(x) ? "?" : "ST_GeomFromText(?)")),
                        DataChangeRecord.KeyExtractor.simple(),
                        new OceanBaseRecordFlusher(connectorOptions));

        List<String> values =
                Arrays.asList(
                        "POINT(1 1)",
                        "POLYGON((1 1,2 1,2 2,1 2,1 1))",
                        "LINESTRING(3 0,3 3,3 5)",
                        "POLYGON((1 1,2 1,2 2,1 2,1 1))",
                        "MULTIPOINT((1 1),(2 2))",
                        "MULTILINESTRING((1 1,2 2,3 3),(4 4,5 5))",
                        "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7,5 5)))",
                        "GEOMETRYCOLLECTION(POINT(10 10),POINT(30 30),LINESTRING(15 15,20 20))");

        GenericRowData rowData = new GenericRowData(RowKind.INSERT, values.size() + 1);
        rowData.setField(0, 1);
        for (int i = 0; i < values.size(); i++) {
            rowData.setField(i + 1, StringData.fromString(values.get(i)));
        }

        env.fromElements((RowData) rowData).sinkTo(sink);
        env.execute();

        List<String> actual =
                queryTable(
                        "gis_types",
                        Arrays.asList(
                                "id",
                                "ST_AsWKT(point_c)",
                                "ST_AsWKT(geometry_c)",
                                "ST_AsWKT(linestring_c)",
                                "ST_AsWKT(polygon_c)",
                                "ST_AsWKT(multipoint_c)",
                                "ST_AsWKT(multiline_c)",
                                "ST_AsWKT(multipolygon_c)",
                                "ST_AsWKT(geometrycollection_c)"));

        assertEquals("1," + String.join(",", values), actual.get(0));

        dropTables("gis_types");
    }

    @Test
    public void testMultipleTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OceanBaseConnectorOptions connectorOptions =
                new OceanBaseConnectorOptions(getBaseOptions());
        OceanBaseSink<OceanBaseTestData> sink =
                new OceanBaseSink<>(
                        connectorOptions,
                        null,
                        new OceanBaseTestDataSerializationSchema(),
                        DataChangeRecord.KeyExtractor.simple(),
                        new OceanBaseRecordFlusher(connectorOptions));

        OceanBaseDialect dialect = new OceanBaseMySQLDialect();
        String database = getSchemaName();
        String tableA = "table_a";
        String tableB = "table_B";
        String tableC = "TABLE_C";

        String tableFullNameA = dialect.getFullTableName(database, tableA);
        String tableFullNameB = dialect.getFullTableName(database, tableB);
        String tableFullNameC = dialect.getFullTableName(database, tableC);

        ResolvedSchema schemaA =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.INT().notNull()),
                                Column.physical("a1", DataTypes.INT().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("a")));
        ResolvedSchema schemaB =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("b", DataTypes.INT().notNull()),
                                Column.physical("b1", DataTypes.STRING().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("b")));
        ResolvedSchema schemaC =
                ResolvedSchema.of(
                        Arrays.asList(
                                Column.physical("c", DataTypes.INT().notNull()),
                                Column.physical("c1", DataTypes.STRING().notNull())));

        // create tables and insert rows
        List<OceanBaseTestData> dataSet =
                Arrays.asList(
                        new OceanBaseTestData(
                                database,
                                tableA,
                                SchemaChangeRecord.Type.CREATE,
                                String.format(
                                        "CREATE TABLE %s (a int(10) primary key, a1 int(10))",
                                        tableFullNameA)),
                        new OceanBaseTestData(
                                database,
                                tableB,
                                SchemaChangeRecord.Type.CREATE,
                                String.format(
                                        "CREATE TABLE %s (b int(10) primary key, b1 varchar(20))",
                                        tableFullNameB)),
                        new OceanBaseTestData(
                                database,
                                tableC,
                                SchemaChangeRecord.Type.CREATE,
                                String.format(
                                        "CREATE TABLE %s (c int(10), c1 varchar(20))",
                                        tableFullNameC)),
                        new OceanBaseTestData(
                                "test",
                                tableA,
                                schemaA,
                                GenericRowData.ofKind(RowKind.INSERT, 1, 1)),
                        new OceanBaseTestData(
                                "test",
                                tableB,
                                schemaB,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, 2, StringData.fromString("2"))),
                        new OceanBaseTestData(
                                "test",
                                tableB,
                                schemaB,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, 3, StringData.fromString("3"))),
                        new OceanBaseTestData(
                                "test",
                                tableC,
                                schemaC,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, 4, StringData.fromString("4"))));

        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();

        assertEqualsInAnyOrder(queryTable(tableFullNameA), Collections.singletonList("1,1"));
        assertEqualsInAnyOrder(queryTable(tableFullNameB), Arrays.asList("2,2", "3,3"));
        assertEqualsInAnyOrder(queryTable(tableFullNameC), Collections.singletonList("4,4"));

        // update and delete rows
        dataSet =
                Arrays.asList(
                        new OceanBaseTestData(
                                "test",
                                tableA,
                                schemaA,
                                GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1, 2)),
                        new OceanBaseTestData(
                                "test",
                                tableB,
                                schemaB,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER, 2, StringData.fromString("3"))),
                        new OceanBaseTestData(
                                "test",
                                tableB,
                                schemaB,
                                GenericRowData.ofKind(
                                        RowKind.DELETE, 3, StringData.fromString("3"))));

        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();

        assertEqualsInAnyOrder(queryTable(tableFullNameA), Collections.singletonList("1,2"));
        assertEqualsInAnyOrder(queryTable(tableFullNameB), Collections.singletonList("2,3"));

        // truncate tables
        dataSet =
                Arrays.asList(
                        new OceanBaseTestData(
                                database,
                                tableA,
                                SchemaChangeRecord.Type.TRUNCATE,
                                String.format("TRUNCATE TABLE %s", tableFullNameA)),
                        new OceanBaseTestData(
                                database,
                                tableA,
                                SchemaChangeRecord.Type.TRUNCATE,
                                String.format("TRUNCATE TABLE %s", tableFullNameB)));
        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();

        assertTrue(CollectionUtils.isEmpty(queryTable(tableFullNameA)));
        assertTrue(CollectionUtils.isEmpty(queryTable(tableFullNameB)));

        // drop tables
        dataSet =
                Arrays.asList(
                        new OceanBaseTestData(
                                database,
                                tableA,
                                SchemaChangeRecord.Type.DROP,
                                String.format("DROP TABLE %s ", tableFullNameA)),
                        new OceanBaseTestData(
                                database,
                                tableB,
                                SchemaChangeRecord.Type.CREATE,
                                String.format("DROP TABLE %s ", tableFullNameB)),
                        new OceanBaseTestData(
                                database,
                                tableC,
                                SchemaChangeRecord.Type.CREATE,
                                String.format("DROP TABLE %s ", tableFullNameC)));
        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();
    }
}
