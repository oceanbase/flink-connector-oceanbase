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
package com.oceanbase.connector.flink.source.cdc.mysql;

import com.oceanbase.connector.flink.source.FieldType;
import com.oceanbase.connector.flink.source.JdbcMetadataAccessor;

import org.apache.flink.util.function.SupplierWithException;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static com.oceanbase.connector.flink.source.cdc.mysql.MysqlTypeConverter.toOceanBaseType;
import static com.oceanbase.connector.flink.table.OceanBaseTypeMapper.convertToLogicalType;

public class MysqlMetadataAccessor extends JdbcMetadataAccessor {

    private final SupplierWithException<DatabaseMetaData, SQLException> metadataSupplier;

    public MysqlMetadataAccessor(
            SupplierWithException<DatabaseMetaData, SQLException> metadataSupplier) {
        this.metadataSupplier = metadataSupplier;
    }

    @Override
    public SupplierWithException<DatabaseMetaData, SQLException> getMetadataSupplier() {
        return metadataSupplier;
    }

    @Override
    public FieldType getFieldType(int jdbcType, String jdbcTypeName, int precision, int scale) {
        return new FieldType(
                convertToLogicalType(jdbcType, precision, scale),
                toOceanBaseType(jdbcTypeName, precision, scale));
    }
}
