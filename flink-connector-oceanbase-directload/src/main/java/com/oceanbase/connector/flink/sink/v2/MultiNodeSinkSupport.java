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
import com.oceanbase.connector.flink.table.RecordSerializationSchema;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;

abstract class MultiNodeSinkSupport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String executionId;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final RecordSerializationSchema<RowData> recordSerializer;

    protected MultiNodeSinkSupport(
            String executionId,
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<RowData> recordSerializer) {
        this.executionId = executionId;
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
    }

    protected final MultiNodeWriter createWriterInternal() {
        return new MultiNodeWriter(executionId, connectorOptions, recordSerializer);
    }

    protected final Committer<String> createCommitterInternal() throws IOException {
        return new NoOPCommiter(executionId, connectorOptions);
    }

    public final SimpleVersionedSerializer<String> getCommittableSerializer() {
        return new NoOpSerializer();
    }

    public final void addPostCommitTopology(DataStream<CommittableMessage<String>> commits) {
        commits.rebalance()
                .sinkTo(new GlobalCommiter(executionId, connectorOptions))
                .setParallelism(1)
                .setDescription("Global Committer Node");
    }
}
