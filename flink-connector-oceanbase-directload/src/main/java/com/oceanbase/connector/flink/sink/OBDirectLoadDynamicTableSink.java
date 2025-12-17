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
import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.directload.DirectLoader;
import com.oceanbase.connector.flink.sink.v2.MultiNodeSink;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The direct-load dynamic table sink. see {@link DynamicTableSink}. */
public class OBDirectLoadDynamicTableSink implements DynamicTableSink {
    private static final Logger LOG = LoggerFactory.getLogger(OBDirectLoadDynamicTableSink.class);
    private final ResolvedSchema physicalSchema;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final int numberOfTaskSlots;

    public OBDirectLoadDynamicTableSink(
            ResolvedSchema physicalSchema,
            OBDirectLoadConnectorOptions connectorOptions,
            int numberOfTaskSlots) {
        this.physicalSchema = physicalSchema;
        this.connectorOptions = connectorOptions;
        this.numberOfTaskSlots = numberOfTaskSlots;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        TableId tableId =
                new TableId(connectorOptions.getSchemaName(), connectorOptions.getTableName());
        OceanBaseRowDataSerializationSchema serializationSchema =
                new OceanBaseRowDataSerializationSchema(new TableInfo(tableId, physicalSchema));
        if (connectorOptions.getBoundedModeEnabled()) {
            if (!context.isBounded()) {
                throw new NotImplementedException(
                        "The direct-load currently only supports running with bounded source.");
            }
        }
        return new DirectLoadStreamSinkProvider(
                (dataStream) -> {
                    // Get direct-load's execution id
                    DirectLoader directLoader =
                            DirectLoadUtils.buildDirectLoaderFromConnOption(connectorOptions, null);
                    String executionId = directLoader.begin();
                    directLoader.close();

                    return dataStream.sinkTo(
                            new MultiNodeSink(executionId, connectorOptions, serializationSchema));
                });
    }

    @Override
    public DynamicTableSink copy() {
        return new OBDirectLoadDynamicTableSink(
                physicalSchema, connectorOptions, numberOfTaskSlots);
    }

    @Override
    public String asSummaryString() {
        return OBDirectLoadDynamicTableSinkFactory.IDENTIFIER;
    }
}
