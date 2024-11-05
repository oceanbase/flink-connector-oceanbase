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

package com.oceanbase.connector.flink.tests;

import com.oceanbase.connector.flink.tests.utils.FlinkContainerTestEnvironment;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OceanBaseE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseE2eITCase.class);

    private static final String SINK_CONNECTOR_NAME = "flink-sql-connector-oceanbase.jar";

    @BeforeClass
    public static void setup() {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
    }

    @AfterClass
    public static void tearDown() {
        CONTAINER.stop();
    }

    @Before
    public void before() throws Exception {
        super.before();

        initialize("sql/products.sql");
    }

    @After
    public void after() throws Exception {
        super.after();

        dropTables("products");
    }

    @Test
    public void testInsertValues() throws Exception {
        List<String> sqlLines = new ArrayList<>();

        sqlLines.add("SET 'execution.checkpointing.interval' = '3s';");

        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='oceanbase',"
                                + "  'url'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='products'"
                                + ");",
                        getJdbcUrl(), getUsername(), getPassword(), getSchemaName()));

        sqlLines.add(
                "INSERT INTO target "
                        + "VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14),"
                        + "       (102, 'car battery', '12V car battery', 8.1),"
                        + "       (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),"
                        + "       (104, 'hammer', '12oz carpenter''s hammer', 0.75),"
                        + "       (105, 'hammer', '14oz carpenter''s hammer', 0.875),"
                        + "       (106, 'hammer', '16oz carpenter''s hammer', 1.0),"
                        + "       (107, 'rocks', 'box of assorted rocks', 5.3),"
                        + "       (108, 'jacket', 'water resistent black wind breaker', 0.1),"
                        + "       (109, 'spare tire', '24 inch spare tire', 22.2);");

        submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME));
        waitUntilJobRunning(Duration.ofSeconds(30));

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
}
