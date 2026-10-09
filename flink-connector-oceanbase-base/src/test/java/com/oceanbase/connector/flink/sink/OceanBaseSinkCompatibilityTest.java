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

package com.oceanbase.connector.flink.sink;

import com.oceanbase.connector.flink.ConnectorOptions;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Verifies the version-specific sink adapter against both Flink 1.x and Flink 2.x. */
public class OceanBaseSinkCompatibilityTest {

    @Test
    public void testUsesInputSerializerWhenObjectReuseIsEnabled() throws Exception {
        AtomicInteger serializerRequests = new AtomicInteger();
        SinkWriter<Integer> writer =
                createWriter(null, true, IntSerializer.INSTANCE, serializerRequests);

        assertSame(IntSerializer.INSTANCE, getTypeSerializer(writer));
        assertEquals(1, serializerRequests.get());
        writer.close();
    }

    @Test
    public void testSkipsInputSerializerWhenObjectReuseIsDisabled() throws Exception {
        AtomicInteger serializerRequests = new AtomicInteger();
        SinkWriter<Integer> writer =
                createWriter(null, false, IntSerializer.INSTANCE, serializerRequests);

        assertNull(getTypeSerializer(writer));
        assertEquals(0, serializerRequests.get());
        writer.close();
    }

    @Test
    public void testConfiguredSerializerTakesPrecedence() throws Exception {
        AtomicInteger serializerRequests = new AtomicInteger();
        SinkWriter<Integer> writer =
                createWriter(IntSerializer.INSTANCE, true, null, serializerRequests);

        assertSame(IntSerializer.INSTANCE, getTypeSerializer(writer));
        assertEquals(1, serializerRequests.get());
        writer.close();
    }

    private static SinkWriter<Integer> createWriter(
            TypeSerializer<Integer> configuredSerializer,
            boolean objectReuseEnabled,
            TypeSerializer<Integer> inputSerializer,
            AtomicInteger serializerRequests)
            throws Exception {
        OceanBaseSink<Integer> sink =
                new OceanBaseSink<>(
                        connectorOptions(),
                        configuredSerializer,
                        noOpRecordSerializer(),
                        DataChangeRecord.KeyExtractor.simple(),
                        noOpRecordFlusher());

        Class<?> contextClass = getWriterContextClass();
        SinkWriterMetricGroup metricGroup = proxy(SinkWriterMetricGroup.class);
        Object context =
                Proxy.newProxyInstance(
                        contextClass.getClassLoader(),
                        new Class<?>[] {contextClass},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "metricGroup":
                                    return metricGroup;
                                case "isObjectReuseEnabled":
                                    return objectReuseEnabled;
                                case "createInputSerializer":
                                    serializerRequests.incrementAndGet();
                                    return inputSerializer;
                                default:
                                    return defaultValue(method.getReturnType());
                            }
                        });

        Method createWriter = OceanBaseSink.class.getMethod("createWriter", contextClass);
        @SuppressWarnings("unchecked")
        SinkWriter<Integer> writer = (SinkWriter<Integer>) createWriter.invoke(sink, context);
        return writer;
    }

    private static ConnectorOptions connectorOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(ConnectorOptions.SYNC_WRITE.key(), "true");
        return new ConnectorOptions(options) {};
    }

    private static RecordSerializationSchema<Integer> noOpRecordSerializer() {
        return value -> null;
    }

    private static RecordFlusher noOpRecordFlusher() {
        return new RecordFlusher() {
            @Override
            public void flush(List<DataChangeRecord> records) {}

            @Override
            public void close() {}
        };
    }

    private static Class<?> getWriterContextClass() throws ClassNotFoundException {
        try {
            return Class.forName("org.apache.flink.api.connector.sink2.WriterInitContext");
        } catch (ClassNotFoundException ignored) {
            return Class.forName("org.apache.flink.api.connector.sink2.Sink$InitContext");
        }
    }

    private static TypeSerializer<?> getTypeSerializer(SinkWriter<?> writer) throws Exception {
        Field field = OceanBaseWriter.class.getDeclaredField("typeSerializer");
        field.setAccessible(true);
        return (TypeSerializer<?>) field.get(writer);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> interfaceClass) {
        return (T)
                Proxy.newProxyInstance(
                        interfaceClass.getClassLoader(),
                        new Class<?>[] {interfaceClass},
                        (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        return 0D;
    }
}
