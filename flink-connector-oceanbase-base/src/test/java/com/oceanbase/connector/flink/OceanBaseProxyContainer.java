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

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Objects;

public class OceanBaseProxyContainer extends JdbcDatabaseContainer<OceanBaseProxyContainer> {

    private static final String IMAGE = "oceanbase/obproxy-ce";

    private static final int SQL_PORT = 2883;
    private static final int RPC_PORT = 2885;
    private static final String APP_NAME = "flink_oceanbase_test";

    private String configUrl;
    private String password;
    private String proxyroPassword;

    public OceanBaseProxyContainer(String version) {
        super(DockerImageName.parse(IMAGE + ":" + version));
        addExposedPorts(SQL_PORT, RPC_PORT);
    }

    @Override
    protected void configure() {
        addEnv("APP_NAME", APP_NAME);
        addEnv("CONFIG_URL", Objects.requireNonNull(configUrl));
        addEnv("PROXYSYS_PASSWORD", Objects.requireNonNull(password));
        addEnv("PROXYRO_PASSWORD", Objects.requireNonNull(proxyroPassword));
    }

    public OceanBaseProxyContainer withConfigUrl(String configUrl) {
        this.configUrl = configUrl;
        return this;
    }

    public OceanBaseProxyContainer withPassword(String password) {
        this.password = password;
        return this;
    }

    public OceanBaseProxyContainer withProxyroPassword(String proxyroPassword) {
        this.proxyroPassword = proxyroPassword;
        return this;
    }

    @Override
    public String getUsername() {
        return "root@proxysys";
    }

    @Override
    public String getPassword() {
        return password;
    }

    public int getSqlPort() {
        return getMappedPort(SQL_PORT);
    }

    public int getRpcPort() {
        return getMappedPort(RPC_PORT);
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:mysql://" + getHost() + ":" + getSqlPort();
    }

    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    protected String getTestQueryString() {
        return "SHOW PROXYCONFIG LIKE 'obproxy_config_server_url'";
    }
}
