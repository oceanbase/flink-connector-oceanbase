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

import org.apache.flink.util.function.SupplierWithException;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** The generic Jdbc MetadataAccessor. */
public abstract class JdbcMetadataAccessor implements MetadataAccessor {

    public abstract SupplierWithException<DatabaseMetaData, SQLException> getMetadataSupplier();

    public abstract FieldType getFieldType(
            int jdbcType, String jdbcTypeName, int precision, int scale);

    public List<FieldSchema> getColumnInfo(
            String databaseName, String schemaName, String tableName) {

        List<FieldSchema> fieldSchemas = new ArrayList<>();

        try (ResultSet rs =
                getMetadataSupplier().get().getColumns(databaseName, schemaName, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("REMARKS");
                int dataType = rs.getInt("DATA_TYPE");
                String typeName = rs.getString("TYPE_NAME");
                int precision = rs.getInt("COLUMN_SIZE");
                int scale = rs.getInt("DECIMAL_DIGITS");
                String columnDefault = rs.getString("COLUMN_DEF");
                boolean isNullable = rs.getBoolean("IS_NULLABLE");

                fieldSchemas.add(
                        new FieldSchema(
                                fieldName,
                                getFieldType(dataType, typeName, precision, scale),
                                columnDefault,
                                comment,
                                isNullable));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return fieldSchemas;
    }

    public List<String> getPrimaryKeys(String databaseName, String schemaName, String tableName) {
        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs =
                getMetadataSupplier().get().getPrimaryKeys(databaseName, schemaName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return primaryKeys;
    }
}
