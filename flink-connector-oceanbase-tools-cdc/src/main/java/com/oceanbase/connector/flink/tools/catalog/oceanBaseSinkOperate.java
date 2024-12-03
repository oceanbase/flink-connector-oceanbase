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

package com.oceanbase.connector.flink.tools.catalog;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.sink.OceanBaseRecordFlusher;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseJsonSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** OceanBase Sink Operate. */
@Public
public class oceanBaseSinkOperate implements Serializable {
    private static final long serialVersionUID = 1L;
    protected Configuration sinkConfig;
    private final OceanBaseConnectorOptions connectorOptions;

    public oceanBaseSinkOperate(Configuration cdcSinkConfig) {
        sinkConfig = cdcSinkConfig;
        this.connectorOptions = getOceanBaseConnectorOptions();
    }

    @Deprecated
    public static String quoteDefaultValue(String defaultValue) {
        return OceanBaseSchemaFactory.quoteDefaultValue(defaultValue);
    }

    @Deprecated
    public static String quoteComment(String comment) {
        return OceanBaseSchemaFactory.quoteComment(comment);
    }

    @Deprecated
    public static String quoteTableIdentifier(String tableIdentifier) {
        return OceanBaseSchemaFactory.quoteTableIdentifier(tableIdentifier);
    }

    public OceanBaseSink<String> createGenericOceanBaseSink(String schemaName, String tableName)
            throws Exception {

        List<String> columnNames = new ArrayList<>();
        List<String> pkColumnNames = new ArrayList<>();
        List<LogicalType> columnTypes = new ArrayList<>();
        OceanBaseMySQLDialect dialect = new OceanBaseMySQLDialect();
        try (OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions)) {
            Connection connection = connectionProvider.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet columns = metaData.getColumns(schemaName, schemaName, tableName, null)) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    int dataType = columns.getInt("DATA_TYPE");
                    int precision = columns.getInt("COLUMN_SIZE");
                    int scale = columns.getInt("DECIMAL_DIGITS");
                    columnNames.add(columnName);
                    columnTypes.add(
                            OceanBaseTypeMapper.convertToLogicalType(dataType, precision, scale));
                }
            }

            try (ResultSet primaryKeys =
                    metaData.getPrimaryKeys(schemaName, schemaName, tableName)) {
                while (primaryKeys.next()) {
                    String pkColumnName = primaryKeys.getString("COLUMN_NAME");
                    pkColumnNames.add(pkColumnName);
                }
            }
        }
        TableInfo tableInfo =
                new TableInfo(
                        new TableId(dialect::getFullTableName, schemaName, tableName),
                        pkColumnNames,
                        columnNames,
                        columnTypes,
                        null);

        return new OceanBaseSink<>(
                connectorOptions,
                null,
                new OceanBaseJsonSerializationSchema(tableInfo),
                DataChangeRecord.KeyExtractor.simple(),
                new OceanBaseRecordFlusher(connectorOptions));
    }

    public OceanBaseConnectorOptions getOceanBaseConnectorOptions() {
        Map<String, String> options = new HashMap<>();
        if (sinkConfig != null) {
            options = sinkConfig.toMap();
            return new OceanBaseConnectorOptions(options);
        }
        throw new RuntimeException("sinkConfig is null");
    }
}
