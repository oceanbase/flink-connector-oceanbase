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

import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.directload.DirectLoader;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.Arrays;
import java.util.List;

public class OBDirectLoadITCase extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OBDirectLoadITCase.class);

    @BeforeAll
    public static void setup() {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
    }

    @AfterAll
    public static void tearDown() {
        CONTAINER.stop();
    }

    @BeforeEach
    public void before() throws Exception {
        initialize("sql/products.sql");
    }

    @AfterEach
    public void after() throws Exception {
        dropTables("products");
    }

    @Test
    public void testDirectLoadSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createTableSql =
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") with ("
                                + "  'connector' = 'oceanbase-directload',"
                                + "  'host' = '%s', "
                                + "  'port' = '%s', "
                                + "  'schema-name' = 'test', "
                                + "  'table-name' = 'products', "
                                + "  'username' = '%s', "
                                + "  'tenant-name' = '%s', "
                                + "  'password' = '%s' "
                                + ");",
                        getHost(),
                        getRpcPort(),
                        getUserInfo().getUser(),
                        getUserInfo().getTenant(),
                        getPassword());
        tEnv.executeSql(createTableSql);

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
    }

    @Test
    public void testMultiNodeDirectLoadSink() throws Exception {
        // 1. get DirectLoader and execution id.
        DirectLoader directLoad = getDirectLoad();
        String executionId = directLoad.begin();

        // 2. Prepare flink execution env.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createTableSql =
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") with ("
                                + "  'connector' = 'oceanbase-directload',"
                                + "  'host' = '%s', "
                                + "  'port' = '%s', "
                                + "  'schema-name' = 'test', "
                                + "  'table-name' = 'products', "
                                + "  'username' = '%s', "
                                + "  'tenant-name' = '%s', "
                                + "  'password' = '%s', "
                                + "  'execution-id' = '%s', "
                                + "  'enable-multi-node-write' = '%s' "
                                + ");",
                        getHost(),
                        getRpcPort(),
                        getUserInfo().getUser(),
                        getUserInfo().getTenant(),
                        getPassword(),
                        executionId,
                        true);
        tEnv.executeSql(createTableSql);

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

        // 3. After the flink job is finished, commit manually.
        directLoad.commit();

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
    }

    private DirectLoader getDirectLoad() {
        ImmutableMap<String, String> configMap =
                ImmutableMap.of(
                        OBDirectLoadConnectorOptions.HOST.key(),
                        getHost(),
                        OBDirectLoadConnectorOptions.PORT.key(),
                        String.valueOf(getRpcPort()),
                        OBDirectLoadConnectorOptions.SCHEMA_NAME.key(),
                        getSchemaName(),
                        OBDirectLoadConnectorOptions.TABLE_NAME.key(),
                        "products",
                        OBDirectLoadConnectorOptions.USERNAME.key(),
                        getUserInfo().getUser(),
                        OBDirectLoadConnectorOptions.TENANT_NAME.key(),
                        getUserInfo().getTenant(),
                        OBDirectLoadConnectorOptions.PASSWORD.key(),
                        getPassword());
        OBDirectLoadConnectorOptions connectorOptions = new OBDirectLoadConnectorOptions(configMap);
        return DirectLoadUtils.buildDirectLoaderFromConnOption(connectorOptions);
    }
}
