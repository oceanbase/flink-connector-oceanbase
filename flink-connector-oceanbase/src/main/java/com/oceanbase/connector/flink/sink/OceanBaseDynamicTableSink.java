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

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import java.util.stream.Collectors;

public class OceanBaseDynamicTableSink extends AbstractDynamicTableSink {

    private final OceanBaseConnectorOptions connectorOptions;

    public OceanBaseDynamicTableSink(
            ResolvedSchema physicalSchema, OceanBaseConnectorOptions connectorOptions) {
        super(physicalSchema);
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new SinkProvider(
                typeSerializer ->
                        new OceanBaseSink<>(
                                connectorOptions,
                                typeSerializer,
                                new OceanBaseRowDataSerializationSchema(
                                        new TableInfo(
                                                connectorOptions.getSchemaName(),
                                                connectorOptions.getTableName(),
                                                physicalSchema)),
                                record ->
                                        ((TableInfo) record.getTable())
                                                .getPrimaryKey().stream()
                                                        .map(
                                                                key -> {
                                                                    Object value =
                                                                            record.getFieldValue(
                                                                                    key);
                                                                    return value == null
                                                                            ? "null"
                                                                            : value.toString();
                                                                })
                                                        .collect(Collectors.joining("#")),
                                new OceanBaseRecordFlusher(connectorOptions)));
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
