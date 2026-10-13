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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link AbstractDynamicTableSink}. */
public class AbstractDynamicTableSinkTest {

    @Test
    public void testSinkProviderWithParallelism() {
        // Test with parallelism set
        AbstractDynamicTableSink.SinkProvider providerWithParallelism =
                new AbstractDynamicTableSink.SinkProvider(ignored -> null, 4);

        Optional<Integer> parallelism = providerWithParallelism.getParallelism();
        assertTrue(parallelism.isPresent());
        assertEquals(4, parallelism.get());
    }

    @Test
    public void testSinkProviderWithoutParallelism() {
        // Test without parallelism set (null)
        AbstractDynamicTableSink.SinkProvider providerWithoutParallelism =
                new AbstractDynamicTableSink.SinkProvider(ignored -> null, null);

        Optional<Integer> parallelism = providerWithoutParallelism.getParallelism();
        assertFalse(parallelism.isPresent());
    }

    @Test
    public void testSinkProviderWithInvalidParallelism() {
        assertThrows(
                IllegalStateException.class,
                () -> new AbstractDynamicTableSink.SinkProvider(ignored -> null, 0));
        assertThrows(
                IllegalStateException.class,
                () -> new AbstractDynamicTableSink.SinkProvider(ignored -> null, -1));
    }

    @Test
    public void testSinkProviderCreatesSerializerWhenObjectReuseIsEnabled() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().enableObjectReuse();
        DataStream<RowData> dataStream = createDataStream(environment);
        AtomicReference<TypeSerializer<RowData>> serializer = new AtomicReference<>();
        AbstractDynamicTableSink.SinkProvider provider =
                new AbstractDynamicTableSink.SinkProvider(
                        value -> {
                            serializer.set(value);
                            throw new ExpectedException();
                        },
                        null);

        assertThrows(ExpectedException.class, () -> provider.consumeDataStream(null, dataStream));
        assertNotNull(serializer.get());
    }

    @Test
    public void testSinkProviderSkipsSerializerWhenObjectReuseIsDisabled() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> dataStream = createDataStream(environment);
        AtomicReference<TypeSerializer<RowData>> serializer = new AtomicReference<>();
        AbstractDynamicTableSink.SinkProvider provider =
                new AbstractDynamicTableSink.SinkProvider(
                        value -> {
                            serializer.set(value);
                            throw new ExpectedException();
                        },
                        null);

        assertThrows(ExpectedException.class, () -> provider.consumeDataStream(null, dataStream));
        assertNull(serializer.get());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static DataStream<RowData> createDataStream(StreamExecutionEnvironment environment) {
        TypeInformation<RowData> typeInformation =
                (TypeInformation) TypeInformation.of(GenericRowData.class);
        return environment.fromCollection(
                Collections.<RowData>singletonList(GenericRowData.of(1)), typeInformation);
    }

    private static class ExpectedException extends RuntimeException {}
}
