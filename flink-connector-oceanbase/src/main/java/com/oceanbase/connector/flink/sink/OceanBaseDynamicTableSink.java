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

package com.oceanbase.connector.flink.sink;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.connector.flink.table.TransactionRecord;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;

public class OceanBaseDynamicTableSink extends AbstractDynamicTableSink {

    private final OceanBaseConnectorOptions connectorOptions;

    public OceanBaseDynamicTableSink(
            ResolvedSchema physicalSchema, OceanBaseConnectorOptions connectorOptions) {
        super(physicalSchema);
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        TableId tableId =
                new TableId(
                        connectionProvider.getDialect()::getFullTableName,
                        connectorOptions.getSchemaName(),
                        connectorOptions.getTableName());
        OceanBaseRecordFlusher recordFlusher =
                new OceanBaseRecordFlusher(connectorOptions, connectionProvider);
        return new SinkProvider(
                typeSerializer ->
                        new OceanBaseSink<>(
                                connectorOptions,
                                typeSerializer,
                                new OceanBaseRowDataSerializationSchema(
                                        new TableInfo(tableId, physicalSchema)),
                                DataChangeRecord.KeyExtractor.simple(),
                                recordFlusher,
                                getWriterEventListener(recordFlusher, tableId)));
    }

    protected OceanBaseWriterEvent.Listener getWriterEventListener(
            RecordFlusher recordFlusher, TableId tableId) {
        return (event) -> {
            try {
                if (event == OceanBaseWriterEvent.INITIALIZED) {
                    recordFlusher.flush(
                            new TransactionRecord(tableId, TransactionRecord.Type.BEGIN));
                }
                if (event == OceanBaseWriterEvent.CLOSING) {
                    recordFlusher.flush(
                            new TransactionRecord(tableId, TransactionRecord.Type.COMMIT));
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to flush transaction record", e);
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new OceanBaseDynamicTableSink(physicalSchema, connectorOptions);
    }

    @Override
    public String asSummaryString() {
        return "OceanBase";
    }
}
