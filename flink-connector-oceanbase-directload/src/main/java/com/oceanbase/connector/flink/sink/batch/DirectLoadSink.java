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

package com.oceanbase.connector.flink.sink.batch;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/** The direct-load sink. see {@link Sink}. */
public class DirectLoadSink implements Sink<RowData> {
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final RecordSerializationSchema<RowData> recordSerializer;
    private final int numberOfTaskSlots;

    public DirectLoadSink(
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<RowData> recordSerializer,
            int numberOfTaskSlots) {
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
        this.numberOfTaskSlots = numberOfTaskSlots;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        return new DirectLoadWriter(connectorOptions, recordSerializer, numberOfTaskSlots);
    }
}
