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

package com.oceanbase.connector.flink.connection;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.tools.catalog.TableSchema;
import com.oceanbase.connector.flink.utils.OceanBaseToolsJdbcUtils;

public class OceanBaseToolsConnectProvider extends OceanBaseConnectionProvider {

    public OceanBaseToolsConnectProvider(OceanBaseConnectorOptions options) {
        super(options);
    }

    public boolean databaseExists(String database) {
        return OceanBaseToolsJdbcUtils.databaseExists(database, this::getConnection);
    }

    public void createDatabase(String database) {
        OceanBaseToolsJdbcUtils.createDatabase(database, this::getConnection);
    }

    public boolean tableExists(String database, String table) {
        return OceanBaseToolsJdbcUtils.tableExists(database, table, this::getConnection);
    }

    public void createTable(TableSchema schema) {
        OceanBaseToolsJdbcUtils.createTable(schema, this::getConnection);
    }
}
