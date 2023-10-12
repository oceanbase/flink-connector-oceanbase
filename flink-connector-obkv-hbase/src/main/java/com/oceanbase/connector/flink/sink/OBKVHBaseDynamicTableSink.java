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

import com.oceanbase.connector.flink.OBKVHBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionOptions;
import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OBKVHBaseTableSchema;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.types.RowKind;

public class OBKVHBaseDynamicTableSink extends AbstractDynamicTableSink {

    private final OBKVHBaseConnectorOptions connectorOptions;

    public OBKVHBaseDynamicTableSink(
            ResolvedSchema resolvedSchema, OBKVHBaseConnectorOptions connectorOptions) {
        super(resolvedSchema);
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        OceanBaseWriterOptions writerOptions = connectorOptions.getWriterOptions();
        OBKVHBaseConnectionOptions connectionOptions = connectorOptions.getConnectionOptions();
        OBKVHBaseConnectionProvider connectionProvider =
                new OBKVHBaseConnectionProvider(connectionOptions);
        OBKVHBaseStatementOptions statementOptions = connectorOptions.getStatementOptions();
        OBKVHBaseTableSchema tableSchema = new OBKVHBaseTableSchema(physicalSchema);
        OBKVHBaseStatementExecutor statementExecutor =
                new OBKVHBaseStatementExecutor(statementOptions, tableSchema, connectionProvider);
        return wrapSinkProvider(new OceanBaseSink(writerOptions, statementExecutor));
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public DynamicTableSink copy() {
        return new OBKVHBaseDynamicTableSink(physicalSchema, connectorOptions);
    }

    @Override
    public String asSummaryString() {
        return "OBKV-HBASE";
    }
}
