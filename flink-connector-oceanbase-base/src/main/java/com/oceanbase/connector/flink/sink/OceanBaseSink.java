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
