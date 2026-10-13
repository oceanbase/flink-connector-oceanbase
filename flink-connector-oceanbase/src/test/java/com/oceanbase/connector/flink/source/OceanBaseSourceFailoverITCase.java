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

package com.oceanbase.connector.flink.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Failover integration tests for OceanBase parallel snapshot source. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
public class OceanBaseSourceFailoverITCase extends OceanBaseSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSourceFailoverITCase.class);

    private static final int EXPECTED_ROW_COUNT = 30;

    @BeforeAll
    public static void setup() throws Exception {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
        MINI_CLUSTER_RESOURCE.before();
    }

    @AfterAll
    public static void tearDown() {
        MINI_CLUSTER_RESOURCE.after();
        CONTAINER.stop();
    }

    @BeforeEach
    public void initTestData() throws Exception {
        initialize("sql/mysql/failover_test.sql");
        initialize("sql/mysql/string_pk_failover_test.sql");
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            for (int i = 1; i <= EXPECTED_ROW_COUNT; i++) {
                stmt.execute(
                        String.format(
                                "INSERT INTO failover_products VALUES (%d, 'prod_%03d', 'Description for product %d', %d.0)",
                                i, i, i, i));
                stmt.execute(
                        String.format(
                                "INSERT INTO string_pk_failover_products VALUES ('A%03d', 'prod_%03d', %d.50)",
                                i, i, i));
            }
        }
    }

    @AfterEach
    public void cleanUp() throws Exception {
        dropTables("failover_products", "string_pk_failover_products");
    }

    @Test
    public void testSnapshotReadWithoutFailover() throws Exception {
        testBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.NONE);
    }

    @Test
    public void testTaskManagerFailoverDuringSnapshot() throws Exception {
        testBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.TM);
    }

    @Test
    public void testJobManagerFailoverDuringSnapshot() throws Exception {
        testBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.JM);
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testBoundedSourceWithFailover(1, FailoverType.TM);
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testBoundedSourceWithFailover(1, FailoverType.JM);
    }

    @Test
    public void testStringPkSnapshotReadWithoutFailover() throws Exception {
        testStringPkBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.NONE);
    }

    @Test
    public void testStringPkTaskManagerFailover() throws Exception {
        testStringPkBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.TM);
    }

    @Test
    public void testStringPkJobManagerFailover() throws Exception {
        testStringPkBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.JM);
    }

    private void testBoundedSourceWithFailover(int parallelism, FailoverType failoverType)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        configureFixedDelayRestart(env);

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE source_products ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'oceanbase',"
                        + " 'table-name' = 'failover_products',"
                        + " 'compatible-mode' = 'MySQL',"
                        + " 'split-size' = '3',"
                        + getOptionsString()
                        + ")");

        TableResult result = tEnv.executeSql("SELECT * FROM source_products");
        try (CloseableIterator<Row> iterator = result.collect()) {
            JobID jobId = result.getJobClient().get().getJobID();

            if (failoverType != FailoverType.NONE && iterator.hasNext()) {
                LOG.info("First row arrived, triggering {} failover", failoverType);
                triggerFailover(
                        failoverType,
                        jobId,
                        MINI_CLUSTER_RESOURCE.getMiniCluster(),
                        () -> sleepMs(200));
            }

            List<String> rows = fetchRows(iterator, EXPECTED_ROW_COUNT);
            assertExactlyOnce(rows);
        }
    }

    private void testStringPkBoundedSourceWithFailover(int parallelism, FailoverType failoverType)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        configureFixedDelayRestart(env);

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE source_string_pk ("
                        + " code STRING NOT NULL,"
                        + " name STRING,"
                        + " price DECIMAL(10, 2),"
                        + " PRIMARY KEY (code) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'oceanbase',"
                        + " 'table-name' = 'string_pk_failover_products',"
                        + " 'compatible-mode' = 'MySQL',"
                        + " 'split-size' = '3',"
                        + getOptionsString()
                        + ")");

        TableResult result = tEnv.executeSql("SELECT * FROM source_string_pk");
        try (CloseableIterator<Row> iterator = result.collect()) {
            JobID jobId = result.getJobClient().get().getJobID();

            if (failoverType != FailoverType.NONE && iterator.hasNext()) {
                LOG.info("First row arrived (string PK), triggering {} failover", failoverType);
                triggerFailover(
                        failoverType,
                        jobId,
                        MINI_CLUSTER_RESOURCE.getMiniCluster(),
                        () -> sleepMs(200));
            }

            List<String> rows = fetchRows(iterator, EXPECTED_ROW_COUNT);
            assertStringPkExactlyOnce(rows);
        }
    }

    private static void configureFixedDelayRestart(StreamExecutionEnvironment env) {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        env.configure(configuration);
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            rows.add(iter.next().toString());
            size--;
        }
        return rows;
    }

    private void assertExactlyOnce(List<String> results) {
        Assertions.assertEquals(
                EXPECTED_ROW_COUNT,
                results.size(),
                "Expected " + EXPECTED_ROW_COUNT + " rows but got " + results.size());

        Set<String> unique = new HashSet<>(results);
        Assertions.assertEquals(
                EXPECTED_ROW_COUNT,
                unique.size(),
                "Found duplicate rows: expected "
                        + EXPECTED_ROW_COUNT
                        + " unique rows but got "
                        + unique.size());

        Set<Integer> ids = new HashSet<>();
        for (String row : results) {
            String content = row.substring(row.indexOf('[') + 1, row.indexOf(','));
            ids.add(Integer.parseInt(content.trim()));
        }
        for (int i = 1; i <= EXPECTED_ROW_COUNT; i++) {
            Assertions.assertTrue(ids.contains(i), "Missing row with id=" + i);
        }
    }

    private void assertStringPkExactlyOnce(List<String> results) {
        Assertions.assertEquals(
                EXPECTED_ROW_COUNT,
                results.size(),
                "Expected " + EXPECTED_ROW_COUNT + " rows but got " + results.size());

        Set<String> unique = new HashSet<>(results);
        Assertions.assertEquals(
                EXPECTED_ROW_COUNT,
                unique.size(),
                "Found duplicate rows: expected "
                        + EXPECTED_ROW_COUNT
                        + " unique rows but got "
                        + unique.size());

        Set<String> codes = new HashSet<>();
        for (String row : results) {
            String content = row.substring(row.indexOf('[') + 1, row.indexOf(','));
            codes.add(content.trim());
        }
        for (int i = 1; i <= EXPECTED_ROW_COUNT; i++) {
            String expectedCode = String.format("A%03d", i);
            Assertions.assertTrue(
                    codes.contains(expectedCode), "Missing row with code=" + expectedCode);
        }
    }

    private static void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }
}
