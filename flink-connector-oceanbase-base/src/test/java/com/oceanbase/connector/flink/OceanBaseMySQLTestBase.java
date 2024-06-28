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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class OceanBaseMySQLTestBase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLTestBase.class);

    protected static final Network NETWORK = Network.newNetwork();

    public static final int SQL_PORT = 2881;
    public static final int RPC_PORT = 2882;
    public static final int CONFIG_SERVER_PORT = 8080;

    public static final String CLUSTER_NAME = "github-action";
    public static final String SYS_USERNAME = "root";
    public static final String SYS_PASSWORD = "123456";
    public static final String TEST_TENANT = "flink";
    public static final String TEST_USERNAME = "root@" + TEST_TENANT;
    public static final String TEST_PASSWORD = "654321";
    public static final String TEST_DATABASE = "test";

    @SuppressWarnings("resource")
    protected static GenericContainer<?> container(String initSqlFile) {
        GenericContainer<?> container =
                new GenericContainer<>("oceanbase/oceanbase-ce")
                        .withNetwork(NETWORK)
                        .withExposedPorts(SQL_PORT, RPC_PORT, CONFIG_SERVER_PORT)
                        .withEnv("MODE", "mini")
                        .withEnv("OB_CLUSTER_NAME", CLUSTER_NAME)
                        .withEnv("OB_SYS_PASSWORD", SYS_PASSWORD)
                        .withEnv("OB_TENANT_NAME", TEST_TENANT)
                        .withEnv("OB_TENANT_PASSWORD", TEST_PASSWORD)
                        .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                        .withStartupTimeout(Duration.ofMinutes(4))
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        if (initSqlFile != null) {
            container.withCopyFileToContainer(
                    MountableFile.forClasspathResource(initSqlFile), "/root/boot/init.d/init.sql");
        }
        return container;
    }

    protected abstract Map<String, String> getOptions();

    protected String getJdbcUrl(GenericContainer<?> container) {
        return getJdbcUrl(container.getHost(), container.getMappedPort(SQL_PORT));
    }

    protected String getJdbcUrl(String host, int port) {
        return "jdbc:mysql://" + host + ":" + port + "/" + TEST_DATABASE + "?useSSL=false";
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
