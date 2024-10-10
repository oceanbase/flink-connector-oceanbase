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

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.OBDirectLoadDynamicTableSinkFactory;
import com.oceanbase.connector.flink.sink.batch.DirectLoadSink;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

/** The direct-load dynamic table sink. see {@link DynamicTableSink}. */
public class OBDirectLoadDynamicTableSink implements DynamicTableSink {

    private final ResolvedSchema physicalSchema;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final RuntimeExecutionMode runtimeExecutionMode;
    private final int numberOfTaskSlots;

    public OBDirectLoadDynamicTableSink(
            ResolvedSchema physicalSchema,
            OBDirectLoadConnectorOptions connectorOptions,
            RuntimeExecutionMode runtimeExecutionMode,
            int numberOfTaskSlots) {
        this.physicalSchema = physicalSchema;
        this.connectorOptions = connectorOptions;
        this.runtimeExecutionMode = runtimeExecutionMode;
        this.numberOfTaskSlots = numberOfTaskSlots;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        TableId tableId =
                new TableId(connectorOptions.getSchemaName(), connectorOptions.getTableName());
        DirectLoadSink directLoadSink =
                new DirectLoadSink(
                        connectorOptions,
                        new OceanBaseRowDataSerializationSchema(
                                new TableInfo(tableId, physicalSchema)),
                        numberOfTaskSlots);

        if (context.isBounded()
                && runtimeExecutionMode == RuntimeExecutionMode.BATCH
                && connectorOptions.getEnableMultiNodeWrite()) {
            if (StringUtils.isBlank(connectorOptions.getExecutionId())) {
                throw new RuntimeException(
                        "Execution id can't be null when multi-node-write enable.");
            }
            return new DirectLoadStreamSinkProvider(
                    (dataStream) -> dataStream.sinkTo(directLoadSink));
        } else if (context.isBounded()
                && runtimeExecutionMode == RuntimeExecutionMode.BATCH
                && !connectorOptions.getEnableMultiNodeWrite()) {
            return new DirectLoadStreamSinkProvider(
                    (dataStream) -> dataStream.sinkTo(directLoadSink).setParallelism(1));
        } else {
            throw new NotImplementedException(
                    "The direct-load currently only supports running in flink batch execution mode.");
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new OBDirectLoadDynamicTableSink(
                physicalSchema, connectorOptions, runtimeExecutionMode, numberOfTaskSlots);
    }

    @Override
    public String asSummaryString() {
        return OBDirectLoadDynamicTableSinkFactory.IDENTIFIER;
    }
}
