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

import com.oceanbase.connector.flink.ConnectorOptions;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

public class OceanBaseSink<T> implements Sink<T> {

    private static final long serialVersionUID = 1L;

    private final ConnectorOptions options;
    private final TypeSerializer<T> typeSerializer;
    private final RecordSerializationSchema<T> recordSerializer;
    private final DataChangeRecord.KeyExtractor keyExtractor;
    private final RecordFlusher recordFlusher;

    public OceanBaseSink(
            ConnectorOptions options,
            TypeSerializer<T> typeSerializer,
            RecordSerializationSchema<T> recordSerializer,
            DataChangeRecord.KeyExtractor keyExtractor,
            RecordFlusher recordFlusher) {
        this.options = options;
        this.typeSerializer = typeSerializer;
        this.recordSerializer = recordSerializer;
        this.keyExtractor = keyExtractor;
        this.recordFlusher = recordFlusher;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) {
        return new OceanBaseWriter<>(
                options, context, typeSerializer, recordSerializer, keyExtractor, recordFlusher);
    }
}
