/*
 * Copyright (c) 2023 OceanBase
 * flink-connector-oceanbase is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.oceanbase.connector.flink.table.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.oceanbase.connector.flink.connection.OceanBaseConnectionPool;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.sink.OceanBaseWriterOptions;
import com.oceanbase.connector.flink.table.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.table.OceanBaseTableSchema;

import static org.apache.flink.util.Preconditions.checkState;

public class OceanBaseDynamicTableSink implements DynamicTableSink {

    private final ResolvedSchema physicalSchema;
    private final OceanBaseConnectorOptions connectorOptions;

    public OceanBaseDynamicTableSink(
            ResolvedSchema physicalSchema, OceanBaseConnectorOptions connectorOptions) {
        this.physicalSchema = physicalSchema;
        this.connectorOptions = connectorOptions;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || physicalSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        OceanBaseWriterOptions writerOptions = connectorOptions.getWriterOptions();
        OceanBaseTableSchema tableSchema = new OceanBaseTableSchema(physicalSchema);
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionPool(connectorOptions.getConnectionOptions());
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                final boolean objectReuse =
                        dataStream.getExecutionEnvironment().getConfig().isObjectReuseEnabled();
                TypeSerializer<RowData> typeSerializer =
                        objectReuse
                                ? dataStream
                                        .getType()
                                        .createSerializer(dataStream.getExecutionConfig())
                                : null;
                Sink<RowData> sink =
                        new OceanBaseSink(
                                typeSerializer, writerOptions, tableSchema, connectionProvider);
                return dataStream.sinkTo(sink);
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
