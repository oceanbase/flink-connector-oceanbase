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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

/** The direct-load sink. see {@link Sink}. */
public class MultiNodeDSink implements Sink<Void> {
    private final String executionId;
    private final OBDirectLoadConnectorOptions connectorOptions;

    public MultiNodeDSink(String executionId, OBDirectLoadConnectorOptions connectorOptions) {
        this.executionId = executionId;
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkWriter<Void> createWriter(InitContext context) throws IOException {
        return new MultiNodeCommiter(executionId, connectorOptions);
    }
}
