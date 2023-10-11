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
import com.oceanbase.connector.flink.connection.OceanBaseConnectionPool;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OceanBaseTableSchema;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;

public class OceanBaseDynamicTableSink extends AbstractDynamicTableSink {

    private final OceanBaseConnectorOptions connectorOptions;

    public OceanBaseDynamicTableSink(
            ResolvedSchema physicalSchema, OceanBaseConnectorOptions connectorOptions) {
        super(physicalSchema);
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        OceanBaseWriterOptions writerOptions = connectorOptions.getWriterOptions();
        OceanBaseStatementOptions statementOptions = connectorOptions.getStatementOptions();
        OceanBaseTableSchema tableSchema = new OceanBaseTableSchema(physicalSchema);
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionPool(connectorOptions.getConnectionOptions());
        OceanBaseStatementExecutor statementExecutor =
                new OceanBaseStatementExecutor(statementOptions, tableSchema, connectionProvider);
        return wrapSinkProvider(new OceanBaseSink(writerOptions, statementExecutor));
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
