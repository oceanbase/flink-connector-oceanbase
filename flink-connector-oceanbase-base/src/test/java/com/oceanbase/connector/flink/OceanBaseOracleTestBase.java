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

import java.util.Map;

public abstract class OceanBaseOracleTestBase extends OceanBaseTestBase {

    @Override
    public Map<String, String> getOptions() {
        Map<String, String> options = super.getOptions();
        options.put("driver-class-name", "com.oceanbase.jdbc.Driver");
        return options;
    }

    @Override
    public String getHost() {
        return System.getenv("HOST");
    }

    @Override
    public int getPort() {
        return Integer.parseInt(System.getenv("PORT"));
    }

    @Override
    public int getRpcPort() {
        return Integer.parseInt(System.getenv("RPC_PORT"));
    }

    @Override
    public String getJdbcUrl() {
        return String.format("jdbc:oceanbase://%s:%d/%s", getHost(), getPort(), getSchemaName());
    }

    @Override
    public String getClusterName() {
        return System.getenv("CLUSTER_NAME");
    }

    @Override
    public String getSchemaName() {
        return System.getenv("SCHEMA_NAME");
    }

    @Override
    public String getSysUsername() {
        return System.getenv("SYS_USERNAME");
    }

    @Override
    public String getSysPassword() {
        return System.getenv("SYS_PASSWORD");
    }

    @Override
    public String getUsername() {
        return System.getenv("USERNAME");
    }

    @Override
    public String getPassword() {
        return System.getenv("PASSWORD");
    }
}
