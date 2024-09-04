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

import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.oceanbase.OceanBaseCEContainer;

import java.time.Duration;

public abstract class OceanBaseMySQLTestBase extends OceanBaseTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLTestBase.class);

    private static final int SQL_PORT = 2881;
    private static final int RPC_PORT = 2882;
    private static final int CONFIG_SERVER_PORT = 8080;

    private static final String CLUSTER_NAME = "flink-oceanbase-ci";
    private static final String TEST_TENANT = "flink";
    private static final String SYS_PASSWORD = "123456";
    private static final String TEST_PASSWORD = "654321";

    protected static final Network NETWORK = Network.newNetwork();

    @ClassRule
    public static final OceanBaseCEContainer CONTAINER =
            new OceanBaseCEContainer("oceanbase/oceanbase-ce:latest")
                    .withMode(OceanBaseCEContainer.Mode.MINI)
                    .withNetwork(NETWORK)
                    .withTenantName(TEST_TENANT)
                    .withPassword(TEST_PASSWORD)
                    .withExposedPorts(SQL_PORT, RPC_PORT, CONFIG_SERVER_PORT)
                    .withEnv("OB_CLUSTER_NAME", CLUSTER_NAME)
                    .withEnv("OB_SYS_PASSWORD", SYS_PASSWORD)
                    .withStartupTimeout(Duration.ofMinutes(4))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Override
    public String getHost() {
        return CONTAINER.getHost();
    }

    @Override
    public int getPort() {
        return CONTAINER.getMappedPort(SQL_PORT);
    }

    @Override
    public int getRpcPort() {
        return CONTAINER.getMappedPort(RPC_PORT);
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getPort()
                + "/"
                + getSchemaName()
                + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    }

    @Override
    public String getClusterName() {
        return CLUSTER_NAME;
    }

    @Override
    public String getSchemaName() {
        return CONTAINER.getDatabaseName();
    }

    @Override
    public String getSysUsername() {
        return "root";
    }

    @Override
    public String getSysPassword() {
        return SYS_PASSWORD;
    }

    @Override
    public String getUsername() {
        return CONTAINER.getUsername();
    }

    @Override
    public String getPassword() {
        return CONTAINER.getPassword();
    }
}
