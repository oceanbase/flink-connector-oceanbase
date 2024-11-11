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

package com.oceanbase.connector.flink.tools.cdc.mysql;

import com.oceanbase.connector.flink.tools.cdc.JdbcSourceSchema;

import java.sql.DatabaseMetaData;

public class MysqlSchema extends JdbcSourceSchema {

    public MysqlSchema(
            DatabaseMetaData metaData, String databaseName, String tableName, String tableComment)
            throws Exception {
        super(metaData, databaseName, null, tableName, tableComment);
    }

    public String convertToOceanBaseType(String fieldType, Integer precision, Integer scale) {
        return MysqlType.toOceanBaseType(fieldType, precision, scale);
    }

    @Override
    public String getCdcTableName() {
        return databaseName + "\\." + tableName;
    }
}
