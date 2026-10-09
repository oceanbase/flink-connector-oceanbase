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
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies the direct-load adapters against both Flink 1.x and Flink 2.x APIs. */
public class SinkV2CompatibilityTest {

    @Test
    public void testVersionSpecificSinkInterfacesAndMethods() throws Exception {
        boolean flink2 = isFlink2();
        Class<?> writerContext = getWriterContextClass();

        assertTrue(Sink.class.isAssignableFrom(MultiNodeSink.class));
        assertTrue(Sink.class.isAssignableFrom(GlobalCommiter.class));
        assertTrue(
                Class.forName(
                                flink2
                                        ? "org.apache.flink.api.connector.sink2.CommittingSinkWriter"
                                        : "org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink$PrecommittingSinkWriter")
                        .isAssignableFrom(MultiNodeWriter.class));
        assertTrue(
                Class.forName(
                                flink2
                                        ? "org.apache.flink.api.connector.sink2.SupportsCommitter"
                                        : "org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink")
                        .isAssignableFrom(MultiNodeSink.class));
        assertTrue(
                Class.forName(
                                flink2
                                        ? "org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology"
                                        : "org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology")
                        .isAssignableFrom(MultiNodeSink.class));

        Method sinkCreateWriter = MultiNodeSink.class.getMethod("createWriter", writerContext);
        assertTrue(SinkWriter.class.isAssignableFrom(sinkCreateWriter.getReturnType()));
        Method committerCreateWriter =
                GlobalCommiter.class.getMethod("createWriter", writerContext);
        assertTrue(SinkWriter.class.isAssignableFrom(committerCreateWriter.getReturnType()));

        Method createCommitter;
        if (flink2) {
            Class<?> context =
                    Class.forName("org.apache.flink.api.connector.sink2.CommitterInitContext");
            createCommitter = MultiNodeSink.class.getMethod("createCommitter", context);
        } else {
            createCommitter = MultiNodeSink.class.getMethod("createCommitter");
        }
        assertEquals(Committer.class, createCommitter.getReturnType());

        MultiNodeSink sink =
                new MultiNodeSink("test-execution-id", connectorOptions(), noOpRecordSerializer());
        Object committerContext =
                flink2
                        ? Proxy.newProxyInstance(
                                createCommitter.getParameterTypes()[0].getClassLoader(),
                                createCommitter.getParameterTypes(),
                                (proxy, method, args) -> null)
                        : null;
        Object committer =
                flink2
                        ? createCommitter.invoke(sink, committerContext)
                        : createCommitter.invoke(sink);
        assertTrue(committer instanceof NoOPCommiter);
    }

    @Test
    public void testCommittableSerializerIsAvailableForBothVersions() throws Exception {
        MultiNodeSink sink =
                new MultiNodeSink("test-execution-id", connectorOptions(), noOpRecordSerializer());

        SimpleVersionedSerializer<String> serializer = sink.getCommittableSerializer();
        assertTrue(serializer instanceof NoOpSerializer);
        assertEquals(0, serializer.getVersion());
        assertArrayEquals(new byte[0], serializer.serialize("value"));
        assertEquals("", serializer.deserialize(serializer.getVersion(), new byte[0]));
    }

    @Test
    public void testGlobalCommitterWriterUsesVersionSpecificContext() throws Exception {
        Method createWriter =
                GlobalCommiter.class.getMethod("createWriter", getWriterContextClass());
        assertTrue(SinkWriter.class.isAssignableFrom(createWriter.getReturnType()));

        Class<?> contextClass = createWriter.getParameterTypes()[0];
        Object context =
                Proxy.newProxyInstance(
                        contextClass.getClassLoader(),
                        new Class<?>[] {contextClass},
                        (proxy, method, args) -> null);
        GlobalCommiter globalCommitter =
                new GlobalCommiter("test-execution-id", connectorOptions());
        Object writer = createWriter.invoke(globalCommitter, context);

        assertTrue(writer instanceof GlobalCommiterWriter);
    }

    private static boolean isFlink2() {
        try {
            Class.forName("org.apache.flink.api.connector.sink2.WriterInitContext");
            return true;
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    private static Class<?> getWriterContextClass() throws ClassNotFoundException {
        return Class.forName(
                isFlink2()
                        ? "org.apache.flink.api.connector.sink2.WriterInitContext"
                        : "org.apache.flink.api.connector.sink2.Sink$InitContext");
    }

    private static OBDirectLoadConnectorOptions connectorOptions() {
        return new OBDirectLoadConnectorOptions(Collections.emptyMap());
    }

    private static RecordSerializationSchema<RowData> noOpRecordSerializer() {
        return value -> null;
    }
}
