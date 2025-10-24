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

package com.oceanbase.connector.flink.obkv2.sink;

import com.oceanbase.connector.flink.obkv2.OBKVHBase2ConnectorOptions;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

import java.io.Serializable;

/** OBKV HBase2 table sink. */
public class OBKVHBase2TableSink implements DynamicTableSink, Serializable {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final String columnFamily;
    private final ResolvedSchema resolvedSchema;
    private final OBKVHBase2ConnectorOptions connectorOptions;

    public OBKVHBase2TableSink(
            String tableName,
            String columnFamily,
            ResolvedSchema resolvedSchema,
            OBKVHBase2ConnectorOptions connectorOptions) {
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.resolvedSchema = resolvedSchema;
        this.connectorOptions = connectorOptions;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // UPSERT mode: ignore UPDATE_BEFORE
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(
                new OBKVHBase2SinkFunction(
                        tableName, columnFamily, connectorOptions, resolvedSchema));
    }

    @Override
    public DynamicTableSink copy() {
        return new OBKVHBase2TableSink(tableName, columnFamily, resolvedSchema, connectorOptions);
    }

    @Override
    public String asSummaryString() {
        return "obkv-hbase2";
    }
}
