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

package com.oceanbase.connector.flink.sink.v2;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;

import java.io.IOException;

/** Global direct-load committer sink implementation for Flink 2.x. */
public class GlobalCommiter extends GlobalCommiterSupport
        implements Sink<CommittableMessage<String>> {

    public GlobalCommiter(String executionId, OBDirectLoadConnectorOptions connectorOptions) {
        super(executionId, connectorOptions);
    }

    @Override
    public SinkWriter<CommittableMessage<String>> createWriter(WriterInitContext context)
            throws IOException {
        return createWriterInternal();
    }
}
