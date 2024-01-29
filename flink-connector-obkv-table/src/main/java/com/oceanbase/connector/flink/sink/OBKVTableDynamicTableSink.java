/*
 * Copyright (c) 2023 OceanBase.
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

import com.oceanbase.connector.flink.OBKVTableConnectorOptions;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OBKVTableRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;

public class OBKVTableDynamicTableSink extends AbstractDynamicTableSink {

    private final OBKVTableConnectorOptions connectorOptions;

    public OBKVTableDynamicTableSink(
            ResolvedSchema resolvedSchema, OBKVTableConnectorOptions connectorOptions) {
        super(resolvedSchema);
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new SinkProvider(
                typeSerializer ->
                        new OceanBaseSink<>(
                                connectorOptions,
                                typeSerializer,
                                new OBKVTableRowDataSerializationSchema(
                                        new TableInfo(
                                                new TableId(
                                                        connectorOptions.getSchemaName(),
                                                        connectorOptions.getTableName()),
                                                physicalSchema)),
                                DataChangeRecord.KeyExtractor.simple(),
                                new OBKVTableRecordFlusher(connectorOptions)));
    }

    @Override
    public DynamicTableSink copy() {
        return new OBKVTableDynamicTableSink(physicalSchema, connectorOptions);
    }

    @Override
    public String asSummaryString() {
        return "OBKV-TABLE";
    }
}
