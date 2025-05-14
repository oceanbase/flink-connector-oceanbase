package com.oceanbase.connector.flink.sink.v2;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;

import org.apache.flink.api.connector.sink2.Committer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class NoOPCommiter implements Committer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(NoOPCommiter.class);

    public NoOPCommiter(String executionId, OBDirectLoadConnectorOptions connectorOptions) {}

    @Override
    public void commit(Collection<CommitRequest<String>> commits)
            throws IOException, InterruptedException {}

    @Override
    public void close() throws Exception {}
}
