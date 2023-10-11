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
