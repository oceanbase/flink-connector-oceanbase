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
import org.apache.flink.util.function.SerializableSupplier;

import java.io.Serializable;
import java.util.Optional;

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

    static class SinkProvider implements DataStreamSinkProvider, Serializable {

        private static final long serialVersionUID = 1L;

        private final SerializableSupplier<Sink<RowData>> sinkSupplier;
        private final Integer parallelism;

        public SinkProvider(SerializableSupplier<Sink<RowData>> sinkSupplier, Integer parallelism) {
            checkState(
                    parallelism == null || parallelism > 0,
                    "sink.parallelism must be a positive integer, but got: %s",
                    parallelism);
            this.sinkSupplier = sinkSupplier;
            this.parallelism = parallelism;
        }

        @Override
        public Optional<Integer> getParallelism() {
            return Optional.ofNullable(parallelism);
        }

        @Override
        public DataStreamSink<?> consumeDataStream(
                ProviderContext providerContext, DataStream<RowData> dataStream) {
            DataStreamSink<?> dataStreamSink = dataStream.sinkTo(sinkSupplier.get());
            if (parallelism != null) {
                dataStreamSink.setParallelism(parallelism);
            }
            return dataStreamSink;
        }
    }
}
