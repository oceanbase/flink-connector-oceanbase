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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** The direct-load sink. see {@link Sink}. */
public class MultiNodeSink
        implements TwoPhaseCommittingSink<RowData, String>,
                WithPostCommitTopology<RowData, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MultiNodeSink.class);

    private final String executionId;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final RecordSerializationSchema<RowData> recordSerializer;

    public MultiNodeSink(
            String executionId,
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<RowData> recordSerializer) {
        this.executionId = executionId;
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
    }

    @Override
    public PrecommittingSinkWriter<RowData, String> createWriter(InitContext context)
            throws IOException {
        return new MultiNodeWriter(executionId, connectorOptions, recordSerializer);
    }

    @Override
    public Committer<String> createCommitter() throws IOException {
        return new NoOPCommiter(executionId, connectorOptions);
    }

    @Override
    public SimpleVersionedSerializer<String> getCommittableSerializer() {
        return new NoOpSerializer();
    }

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<String>> commits) {
        commits.rebalance()
                .sinkTo(new GlobalCommiter(executionId, connectorOptions))
                .setParallelism(1)
                .setDescription("Global Committer Node");
    }
}
