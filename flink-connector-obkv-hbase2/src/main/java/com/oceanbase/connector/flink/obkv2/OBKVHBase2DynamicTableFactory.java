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

package com.oceanbase.connector.flink.obkv2;

import com.oceanbase.connector.flink.obkv2.sink.OBKVHBase2TableSink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.oceanbase.connector.flink.obkv2.OBKVHBase2ConnectorOptions.*;

/** Dynamic table factory for OBKV HBase2 connector. */
public class OBKVHBase2DynamicTableFactory implements DynamicTableSinkFactory {

    private static final String IDENTIFIER = "obkv-hbase2";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        Map<String, String> options = context.getCatalogTable().getOptions();
        OBKVHBase2ConnectorOptions connectorOptions = new OBKVHBase2ConnectorOptions(options);

        // Validate required options
        if (connectorOptions.getOdpMode()) {
            if (connectorOptions.getOdpIp() == null) {
                throw new IllegalArgumentException("'odp-ip' is required when 'odp-mode' is true");
            }
        } else {
            if (connectorOptions.getUrl() == null
                    || connectorOptions.getSysUsername() == null
                    || connectorOptions.getSysPassword() == null) {
                throw new IllegalArgumentException(
                        "'url', 'sys.username', and 'sys.password' are required when 'odp-mode' is false");
            }
        }

        // Generate column name to index mapping
        Map<String, Integer> columnToIndexMap = generateColumnNameToIndex(resolvedSchema);

        // Set parsed configuration values

        // Timestamp configuration
        String tsColumn = connectorOptions.getTsColumn();
        if (tsColumn != null && !tsColumn.isEmpty()) {
            Integer tsColumnIndex = columnToIndexMap.get(tsColumn);
            if (tsColumnIndex == null) {
                throw new IllegalArgumentException(
                        "Set wrong name of ts column? Column name: " + tsColumn);
            }
            connectorOptions.setTsColumn(tsColumnIndex);
        }

        String tsMap = connectorOptions.getTsMap();
        if (tsMap != null && !tsMap.isEmpty()) {
            Map<Integer, List<Integer>> tsMapParsed = generateTsMapFromStr(columnToIndexMap, tsMap);
            connectorOptions.setTsMap(tsMapParsed);
        }

        return new OBKVHBase2TableSink(
                connectorOptions.getTableName(),
                connectorOptions.getColumnFamily(),
                resolvedSchema,
                connectorOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(SCHEMA_NAME);
        set.add(TABLE_NAME);
        set.add(USERNAME);
        set.add(PASSWORD);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(URL);
        set.add(ODP_MODE);
        set.add(ODP_IP);
        set.add(ODP_PORT);
        set.add(SYS_USERNAME);
        set.add(SYS_PASSWORD);
        set.add(HBASE_PROPERTIES);
        set.add(COLUMN_FAMILY);
        set.add(ROWKEY_DELIMITER);
        set.add(WRITE_PK_VALUE);
        set.add(BUFFER_SIZE);
        set.add(IGNORE_NULL);
        set.add(IGNORE_DELETE);
        set.add(EXCLUDE_UPDATE_COLUMNS);
        set.add(DYNAMIC_COLUMN_SINK);
        set.add(TS_COLUMN);
        set.add(TS_MAP);
        set.add(TS_IN_MILLS);
        set.add(MAX_RETRY_TIMES);
        set.add(RETRY_INTERVAL_MS);
        return set;
    }

    /**
     * Generate a mapping from column name to index.
     *
     * @param schema the resolved schema
     * @return the column name to index mapping
     */
    private static Map<String, Integer> generateColumnNameToIndex(ResolvedSchema schema) {
        Map<String, Integer> map = new HashMap<>();
        List<String> columnNames = schema.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(columnNames.get(i), i);
        }
        return map;
    }

    /**
     * Generate tsMap from string configuration. The format should be:
     * "tsColumn0:column0;tsColumn0:column1;tsColumn1:column2" which means: - tsColumn0 is used as
     * timestamp for column0 and column1 - tsColumn1 is used as timestamp for column2
     *
     * <p>The resulting map is: tsColumn0 -> [column0, column1] tsColumn1 -> [column2]
     *
     * @param columnIndexMap the column name to index mapping
     * @param tsStr the timestamp mapping string
     * @return the timestamp mapping from timestamp column index to list of column indexes
     */
    public static Map<Integer, List<Integer>> generateTsMapFromStr(
            Map<String, Integer> columnIndexMap, String tsStr) {
        if (tsStr == null || tsStr.trim().isEmpty()) {
            return new HashMap<>();
        }

        Map<Integer, List<Integer>> tsMap = new HashMap<>();
        Stream.of(tsStr.split(";"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(
                        ts -> {
                            String[] tsPair = ts.split(":");
                            if (tsPair.length != 2) {
                                throw new IllegalArgumentException(
                                        "Invalid tsMap format. Expected 'tsColumn:column', got: "
                                                + ts);
                            }
                            String tsColumnName = tsPair[0].trim();
                            String columnName = tsPair[1].trim();

                            Integer tsColumnIndex = columnIndexMap.get(tsColumnName);
                            Integer columnIndex = columnIndexMap.get(columnName);

                            if (tsColumnIndex == null) {
                                throw new IllegalArgumentException(
                                        "Timestamp column not found: " + tsColumnName);
                            }
                            if (columnIndex == null) {
                                throw new IllegalArgumentException(
                                        "Data column not found: " + columnName);
                            }

                            tsMap.computeIfAbsent(tsColumnIndex, k -> new ArrayList<>())
                                    .add(columnIndex);
                        });

        return tsMap;
    }
}
