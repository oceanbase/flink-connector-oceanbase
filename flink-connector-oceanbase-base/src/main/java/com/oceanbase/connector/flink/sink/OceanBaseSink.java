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

public class OceanBaseSink implements Sink<RowData> {

    private final OceanBaseWriterOptions writerOptions;
    private final StatementExecutor<RowData> statementExecutor;

    private TypeSerializer<RowData> serializer;

    public OceanBaseSink(
            OceanBaseWriterOptions writerOptions, StatementExecutor<RowData> statementExecutor) {
        this.writerOptions = writerOptions;
        this.statementExecutor = statementExecutor;
    }

    public void setSerializer(TypeSerializer<RowData> serializer) {
        this.serializer = serializer;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) {
        statementExecutor.setContext(context);
        return new OceanBaseWriter<>(writerOptions, serializer, statementExecutor);
    }
}
