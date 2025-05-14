package com.oceanbase.connector.flink.sink.v2;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class NoOpSerializer implements SimpleVersionedSerializer<String> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(String obj) {
        return new byte[0];
    }

    @Override
    public String deserialize(int version, byte[] serialized) {
        return "";
    }
}
