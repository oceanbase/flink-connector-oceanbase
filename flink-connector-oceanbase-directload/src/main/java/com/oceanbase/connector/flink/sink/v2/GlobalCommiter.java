package com.oceanbase.connector.flink.sink.v2;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.directload.DirectLoader;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;

import java.io.IOException;

public class GlobalCommiter implements Sink<CommittableMessage<String>> {
    private final String executionId;
    private final OBDirectLoadConnectorOptions connectorOptions;

    public GlobalCommiter(String executionId, OBDirectLoadConnectorOptions connectorOptions) {
        this.executionId = executionId;
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkWriter<CommittableMessage<String>> createWriter(InitContext context)
            throws IOException {
        return new GlobalCommiterWriter(executionId, connectorOptions);
    }
}

class GlobalCommiterWriter implements SinkWriter<CommittableMessage<String>> {

    private final String executionId;
    private final OBDirectLoadConnectorOptions connectorOptions;

    public GlobalCommiterWriter(String executionId, OBDirectLoadConnectorOptions connectorOptions) {
        this.executionId = executionId;
        this.connectorOptions = connectorOptions;
    }

    @Override
    public void write(CommittableMessage<String> element, Context context)
            throws IOException, InterruptedException {}

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            DirectLoader directLoader =
                    DirectLoadUtils.buildDirectLoaderFromConnOption(connectorOptions, executionId);
            directLoader.begin();
            directLoader.commit();
        }
    }

    @Override
    public void close() throws Exception {}
}
