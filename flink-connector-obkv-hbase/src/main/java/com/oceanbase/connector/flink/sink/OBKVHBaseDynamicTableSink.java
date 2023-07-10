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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.types.RowKind;

import com.oceanbase.connector.flink.OBKVHBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionOptions;
import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OBKVHBaseTableSchema;

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
