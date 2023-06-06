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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OceanBaseTableSchema;
import com.oceanbase.connector.flink.sink.statement.OceanBaseRowDataStatementExecutor;
import com.oceanbase.connector.flink.sink.statement.OceanBaseStatementExecutor;

import java.io.IOException;

public class OceanBaseSink implements Sink<RowData> {

    private final TypeSerializer<RowData> serializer;
    private final OceanBaseWriterOptions writerOptions;
    private final OceanBaseTableSchema tableSchema;
    private final OceanBaseConnectionProvider connectionProvider;

    public OceanBaseSink(
            TypeSerializer<RowData> serializer,
            OceanBaseWriterOptions writerOptions,
            OceanBaseTableSchema tableSchema,
            OceanBaseConnectionProvider connectionProvider) {
        this.serializer = serializer;
        this.writerOptions = writerOptions;
        this.tableSchema = tableSchema;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        OceanBaseStatementExecutor<RowData> statementExecutor =
                new OceanBaseRowDataStatementExecutor(
                        context, writerOptions, tableSchema, connectionProvider);
        return new OceanBaseWriter<>(serializer, writerOptions, statementExecutor);
    }
}
