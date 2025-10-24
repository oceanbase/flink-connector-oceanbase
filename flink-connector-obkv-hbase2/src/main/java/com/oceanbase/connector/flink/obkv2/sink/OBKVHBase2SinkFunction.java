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

package com.oceanbase.connector.flink.obkv2.sink;

import com.oceanbase.connector.flink.obkv2.OBKVHBase2ConnectorOptions;
import com.oceanbase.connector.flink.obkv2.connection.OBKV2ConnectionProvider;
import com.oceanbase.connector.flink.obkv2.util.OBKV2RowDataUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.oceanbase.connector.flink.obkv2.util.OBKV2RowDataUtils.parseTsValueFromRowData;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

/** Sink function for OBKV HBase2 connector. */
public class OBKVHBase2SinkFunction extends RichSinkFunction<RowData>
        implements CheckpointedFunction, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBase2SinkFunction.class);
    private static final Integer USE_TS_COLUMN = -1;

    private final String tableName;
    private final String columnFamily;
    private final byte[] columnFamilyBytes;
    private final OBKVHBase2ConnectorOptions connectorOptions;

    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private final LogicalType[] logicalTypes;
    private final List<String> pkColumns = new ArrayList<>();
    private final List<Integer> pkIndexes = new ArrayList<>();
    private final String[] nonPkColumns;
    private final Set<String> excludeUpdateColumns = new HashSet<>();

    // Field encoders
    private OBKV2RowDataUtils.FieldEncoder[] fieldEncoders;

    // Timestamp configuration
    private Map<Integer, Integer> tsMap = new HashMap<>(); // columnIndex -> tsColumnIndex
    private List<Integer> tsColumnsFromTsMap = new ArrayList<>();
    private Integer tsColumnFromSchema;
    private boolean tsInMills;

    // Buffering
    private final List<Object> mutationList;
    private transient AtomicLong numPendingRequests;

    // Behavior flags
    private boolean ignoreDelete;
    private boolean ignoreNull;
    private boolean dynamicColumnSink;

    // Dynamic sink related
    private int dynamicColumnKeyIndex;
    private int dynamicColumnValueIndex;

    // Connection
    private transient OBKV2ConnectionProvider connectionProvider;
    private transient Table table;

    public OBKVHBase2SinkFunction(
            String tableName,
            String columnFamily,
            OBKVHBase2ConnectorOptions connectorOptions,
            ResolvedSchema resolvedSchema) {
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnFamilyBytes = Bytes.toBytes(columnFamily);
        this.connectorOptions = connectorOptions;

        this.fieldNames = resolvedSchema.getColumnNames().toArray(new String[0]);
        this.fieldTypes = resolvedSchema.getColumnDataTypes().toArray(new DataType[0]);
        this.logicalTypes = new LogicalType[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            this.logicalTypes[i] = fieldTypes[i].getLogicalType();
        }

        // Extract primary key information
        if (!resolvedSchema.getPrimaryKey().isPresent()) {
            throw new IllegalArgumentException(
                    "OBKV HBase2 sink table must have a primary key defined.");
        }
        pkColumns.addAll(resolvedSchema.getPrimaryKey().get().getColumns());
        for (String pkColumn : pkColumns) {
            for (int i = 0; i < fieldNames.length; i++) {
                if (pkColumn.equals(fieldNames[i])) {
                    this.pkIndexes.add(i);
                }
            }
        }

        // Extract non-primary key columns
        ArrayList<String> nonPkColumnsArray = new ArrayList<>(fieldNames.length - pkColumns.size());
        for (String fieldName : fieldNames) {
            if (!pkColumns.contains(fieldName)) {
                nonPkColumnsArray.add(fieldName);
            }
        }
        nonPkColumns = nonPkColumnsArray.toArray(new String[0]);

        Preconditions.checkArgument(
                pkColumns.size() == pkIndexes.size(),
                "Primary key size mismatch: columns="
                        + pkColumns.size()
                        + ", indexes="
                        + pkIndexes.size());

        // Initialize field encoders
        fieldEncoders =
                Arrays.stream(logicalTypes)
                        .map(OBKV2RowDataUtils::createFieldEncoder)
                        .toArray(OBKV2RowDataUtils.FieldEncoder[]::new);

        // Initialize configuration
        this.ignoreDelete = connectorOptions.getIgnoreDelete();
        this.dynamicColumnSink = connectorOptions.getDynamicColumnSink();
        this.ignoreNull = connectorOptions.getIgnoreNull();

        // Initialize exclude columns first, then timestamp config can add timestamp columns to it
        initExcludeUpdateColumns();
        initTsConfig();
        dynamicConfigSanityCheckAndSet();

        Preconditions.checkArgument(
                connectorOptions.getBufferSize() > 0, "Buffer size must be > 0.");
        this.mutationList = new ArrayList<>(connectorOptions.getBufferSize());
        connectionProvider = new OBKV2ConnectionProvider(connectorOptions);
        this.table = connectionProvider.getHTableClient();
    }

    /**
     * Initialize timestamp configuration
     *
     * <p>Timestamp configuration options: 1. tsMap: Fine-grained control with format
     * "tsColumn0:column0;tsColumn0:column1;tsColumn1:column2" - column0 and column1 use tsColumn0
     * as timestamp - column2 uses tsColumn1 as timestamp 2. tsColumn: Global timestamp column, all
     * columns use this column's timestamp 3. tsInMills: Timestamp unit, true=milliseconds,
     * false=seconds
     *
     * <p>Priority: tsColumn > tsMap > system current time
     */
    private void initTsConfig() {
        // Process fine-grained timestamp mapping (tsMap)
        if (null != connectorOptions.getTsMapParsed()
                && !connectorOptions.getTsMapParsed().isEmpty()) {
            // Parse tsMap into column index to timestamp column index mapping
            // Example: tsMap = {1: [0, 2], 3: [4]} means column0 and column2 use column1's
            // timestamp, column4 uses column3's timestamp
            connectorOptions
                    .getTsMapParsed()
                    .forEach((ts, list) -> list.forEach(c -> this.tsMap.put(c, ts)));
            this.tsColumnsFromTsMap.addAll(connectorOptions.getTsMapParsed().keySet());

            // Automatically exclude timestamp columns from being written to HBase
            // These columns are only used for version control, not as data columns
            for (Integer tsColumnIndex : connectorOptions.getTsMapParsed().keySet()) {
                this.excludeUpdateColumns.add(fieldNames[tsColumnIndex]);
            }
        }

        // Set global timestamp column index (tsColumn)
        this.tsColumnFromSchema = connectorOptions.getTsColumnIndex();

        // Automatically exclude global timestamp column from being written to HBase
        if (this.tsColumnFromSchema != null) {
            this.excludeUpdateColumns.add(fieldNames[this.tsColumnFromSchema]);
        }

        // Set timestamp unit (tsInMills: true=milliseconds, false=seconds)
        this.tsInMills = connectorOptions.getTsInMills();
    }

    private void initExcludeUpdateColumns() {
        if (null != connectorOptions.getExcludeUpdateColumns()) {
            String excludeUpdateColumnsString = connectorOptions.getExcludeUpdateColumns();
            this.excludeUpdateColumns.addAll(Arrays.asList(excludeUpdateColumnsString.split(",")));
        }
    }

    private void dynamicConfigSanityCheckAndSet() {
        if (this.dynamicColumnSink) {
            // For dynamic column sink, non-pk columns should be exactly 2
            Preconditions.checkArgument(
                    nonPkColumns.length == 2,
                    "Dynamic column sink requires exactly 2 non-pk columns (columnKey and columnValue)");
            dynamicColumnKeyIndex = pkIndexes.size();
            dynamicColumnValueIndex = this.fieldNames.length - 1;
            Preconditions.checkArgument(
                    fieldTypes.length > dynamicColumnValueIndex, "Invalid field types length");
            Preconditions.checkArgument(
                    logicalTypes[dynamicColumnKeyIndex].getTypeRoot() == VARCHAR,
                    "Dynamic sink column key must be VARCHAR type");
            Preconditions.checkArgument(
                    logicalTypes[dynamicColumnValueIndex].getTypeRoot() == VARCHAR,
                    "Dynamic sink column value must be VARCHAR type");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info(
                "Opening OBKVHBase2SinkFunction for table: {}, columnFamily: {}",
                tableName,
                columnFamily);

        // Initialize connection
        this.connectionProvider = new OBKV2ConnectionProvider(connectorOptions);
        this.table = connectionProvider.getHTableClient();

        this.numPendingRequests = new AtomicLong(0);

        LOG.info("OBKVHBase2SinkFunction opened successfully");
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing OBKVHBase2SinkFunction");

        sync();

        if (connectionProvider != null) {
            connectionProvider.close();
        }

        LOG.info("OBKVHBase2SinkFunction closed");
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        if (value.getRowKind().equals(RowKind.UPDATE_BEFORE)) {
            // Ignore UPDATE_BEFORE
            return;
        }

        if (ignoreDelete && value.getRowKind().equals(RowKind.DELETE)) {
            return;
        }

        synchronized (mutationList) {
            if (value.getRowKind().equals(RowKind.DELETE)) {
                Delete delete = createDelete(value);
                mutationList.add(delete);
            } else {
                Put put = createPut(value);
                mutationList.add(put);
            }
        }

        if (numPendingRequests.incrementAndGet() >= connectorOptions.getBufferSize()) {
            sync();
        }
    }

    private Put createPut(RowData rowData) {
        byte[] rowKey = buildRowKey(rowData);
        Put put = new Put(rowKey);

        // Handle dynamic column sink
        if (dynamicColumnSink) {
            byte[] column =
                    fieldEncoders[dynamicColumnKeyIndex].encode(rowData, dynamicColumnKeyIndex);
            byte[] value =
                    fieldEncoders[dynamicColumnValueIndex].encode(rowData, dynamicColumnValueIndex);
            put.addColumn(columnFamilyBytes, column, value);
            return put;
        }

        // Handle regular columns
        Map<Integer, Long> tsValues = generateTsValues(rowData);
        for (int i = 0; i < fieldNames.length; i++) {
            if (pkIndexes.contains(i) && !connectorOptions.getWritePkValue()) {
                continue;
            }

            // Skip excluded columns
            if (excludeUpdateColumns.contains(fieldNames[i])) {
                continue;
            }

            // Handle null values based on ignoreNull configuration
            // This supports both partial updates and explicit null setting
            if (rowData.isNullAt(i)) {
                if (ignoreNull) {
                    // Skip null columns (for partial updates)
                    continue;
                } else {
                    // Set column to null (explicit null setting)
                    byte[] column = Bytes.toBytes(fieldNames[i]);
                    Long ts = getTsValue(i, tsValues);
                    if (ts != null) {
                        put.addColumn(columnFamilyBytes, column, ts, null);
                    } else {
                        put.addColumn(columnFamilyBytes, column, null);
                    }
                }
            } else {
                // Encode and add column value
                byte[] column = Bytes.toBytes(fieldNames[i]);
                byte[] value = fieldEncoders[i].encode(rowData, i);
                Long ts = getTsValue(i, tsValues);
                if (ts != null) {
                    put.addColumn(columnFamilyBytes, column, ts, value);
                } else {
                    put.addColumn(columnFamilyBytes, column, value);
                }
            }
        }

        return put;
    }

    private Delete createDelete(RowData rowData) {
        byte[] rowKey = buildRowKey(rowData);
        Delete delete = new Delete(rowKey);

        // Handle dynamic column sink
        if (dynamicColumnSink) {
            byte[] column =
                    fieldEncoders[dynamicColumnKeyIndex].encode(rowData, dynamicColumnKeyIndex);
            delete.addColumn(columnFamilyBytes, column);
            return delete;
        }

        // Handle regular columns
        Map<Integer, Long> tsValues = generateTsValues(rowData);
        for (int i = 0; i < fieldNames.length; i++) {
            if (pkIndexes.contains(i) && !connectorOptions.getWritePkValue()) {
                continue;
            } else {
                byte[] column = Bytes.toBytes(fieldNames[i]);
                Long ts = getTsValue(i, tsValues);
                if (ts != null) {
                    delete.addColumn(columnFamilyBytes, column, ts);
                } else {
                    delete.addColumn(columnFamilyBytes, column);
                }
            }
        }

        return delete;
    }

    private byte[] buildRowKey(RowData row) {
        if (pkIndexes.size() == 1) {
            int index = pkIndexes.get(0);
            return fieldEncoders[index].encode(row, index);
        } else {
            // Composite primary key
            byte[] delimiter = Bytes.toBytes(connectorOptions.getRowkeyDelimiter());
            byte[] rowkey = new byte[0];
            for (int i = 0; i < pkIndexes.size() - 1; i++) {
                int index = pkIndexes.get(i);
                if (!row.isNullAt(index)) {
                    rowkey = Bytes.add(rowkey, fieldEncoders[index].encode(row, index), delimiter);
                }
            }
            int lastIndex = pkIndexes.get(pkIndexes.size() - 1);
            if (!row.isNullAt(lastIndex)) {
                rowkey = Bytes.add(rowkey, fieldEncoders[lastIndex].encode(row, lastIndex));
            }
            return rowkey;
        }
    }

    /**
     * Generate timestamp value mapping
     *
     * <p>Timestamp retrieval priority: 1. tsColumn (highest priority) - Global timestamp column 2.
     * tsMap - Fine-grained timestamp mapping 3. System current time (default)
     *
     * @param row Data row
     * @return Column index to timestamp value mapping
     */
    private Map<Integer, Long> generateTsValues(RowData row) {
        Map<Integer, Long> tsValues = new HashMap<>();

        // 1. Check if using tsColumn (highest priority)
        // tsColumn means all columns use the same timestamp column value
        Optional<Long> tsValueFromRow = getTsColumnValue(row);
        if (tsValueFromRow.isPresent()) {
            // If tsColumn is set, all columns use this timestamp value
            tsValues.put(USE_TS_COLUMN, tsValueFromRow.get());
            return tsValues;
        }

        // 2. Use tsMap for fine-grained control
        // tsMap allows different columns to use different timestamp columns
        if (!tsColumnsFromTsMap.isEmpty()) {
            for (Integer index : tsColumnsFromTsMap) {
                if (row.isNullAt(index)) {
                    // If timestamp column is null, use current system time
                    LOG.debug(
                            "Timestamp field {} carries null value, using current time instead",
                            fieldNames[index]);
                    tsValues.put(index, System.currentTimeMillis());
                } else {
                    long fieldValue = row.getLong(index);
                    if (tsInMills) {
                        // Timestamp is already in milliseconds, use directly
                        tsValues.put(index, fieldValue);
                    } else {
                        // Timestamp is in seconds, convert to milliseconds
                        tsValues.put(index, fieldValue * 1000);
                    }
                }
            }
        }

        return tsValues;
    }

    /**
     * Get timestamp value for specified column
     *
     * @param columnIndex Column index
     * @param tsValues Timestamp value mapping
     * @return Timestamp value, null if not found (will use system current time)
     */
    private Long getTsValue(Integer columnIndex, Map<Integer, Long> tsValues) {
        // If tsColumn is used, all columns use the same timestamp value
        if (tsValues.containsKey(USE_TS_COLUMN)) {
            return tsValues.get(USE_TS_COLUMN);
        } else {
            // Otherwise, find corresponding timestamp value based on tsMap
            return tsValues.getOrDefault(tsMap.get(columnIndex), null);
        }
    }

    /**
     * Get timestamp column value from data row
     *
     * @param row Data row
     * @return Timestamp value, empty if column doesn't exist or is null
     */
    private Optional<Long> getTsColumnValue(RowData row) {
        if (null == this.tsColumnFromSchema) {
            return Optional.empty();
        } else {
            Long ts =
                    parseTsValueFromRowData(
                            row, tsColumnFromSchema, logicalTypes[tsColumnFromSchema]);
            if (ts == null) {
                return Optional.empty();
            } else {
                // Convert timestamp unit based on tsInMills configuration
                return Optional.of(tsInMills ? ts : ts * 1000);
            }
        }
    }

    private void sync() throws Exception {
        synchronized (mutationList) {
            if (mutationList.isEmpty()) {
                return;
            }

            LOG.debug("Syncing {} mutations to HBase", mutationList.size());
            long start = System.currentTimeMillis();

            List<Put> puts = new ArrayList<>();
            List<Delete> deletes = new ArrayList<>();

            for (Object mutation : mutationList) {
                if (mutation instanceof Put) {
                    puts.add((Put) mutation);
                } else if (mutation instanceof Delete) {
                    deletes.add((Delete) mutation);
                }
            }

            // Execute puts
            if (!puts.isEmpty()) {
                table.put(puts);
                LOG.debug("Put {} records to HBase", puts.size());
            }

            // Execute deletes
            if (!deletes.isEmpty()) {
                table.delete(deletes);
                LOG.debug("Deleted {} records from HBase", deletes.size());
            }

            long duration = System.currentTimeMillis() - start;
            LOG.debug("Synced {} mutations in {} ms", mutationList.size(), duration);

            numPendingRequests.set(0);
            mutationList.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        do {
            sync();
        } while (numPendingRequests.get() != 0);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {}
}
