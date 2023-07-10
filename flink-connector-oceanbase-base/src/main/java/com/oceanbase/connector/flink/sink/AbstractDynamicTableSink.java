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

package com.oceanbase.connector.flink.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import static org.apache.flink.util.Preconditions.checkState;

public abstract class AbstractDynamicTableSink implements DynamicTableSink {

    protected final ResolvedSchema physicalSchema;

    public AbstractDynamicTableSink(ResolvedSchema physicalSchema) {
        this.physicalSchema = physicalSchema;
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

    protected void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || physicalSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    public DataStreamSinkProvider wrapSinkProvider(OceanBaseSink sink) {
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<RowData> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                final boolean objectReuse =
                        dataStream.getExecutionEnvironment().getConfig().isObjectReuseEnabled();
                TypeSerializer<RowData> typeSerializer =
                        objectReuse
                                ? dataStream
                                        .getType()
                                        .createSerializer(dataStream.getExecutionConfig())
                                : null;
                sink.setSerializer(typeSerializer);
                return dataStream.sinkTo(sink);
            }
        };
    }
}
