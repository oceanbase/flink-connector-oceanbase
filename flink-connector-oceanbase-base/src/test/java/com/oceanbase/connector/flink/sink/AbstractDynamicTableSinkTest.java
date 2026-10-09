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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link AbstractDynamicTableSink}. */
public class AbstractDynamicTableSinkTest {

    @Test
    public void testSinkProviderWithParallelism() {
        // Test with parallelism set
        AbstractDynamicTableSink.SinkProvider providerWithParallelism =
                new AbstractDynamicTableSink.SinkProvider(() -> null, 4);

        Optional<Integer> parallelism = providerWithParallelism.getParallelism();
        assertTrue(parallelism.isPresent());
        assertEquals(4, parallelism.get());
    }

    @Test
    public void testSinkProviderWithoutParallelism() {
        // Test without parallelism set (null)
        AbstractDynamicTableSink.SinkProvider providerWithoutParallelism =
                new AbstractDynamicTableSink.SinkProvider(() -> null, null);

        Optional<Integer> parallelism = providerWithoutParallelism.getParallelism();
        assertFalse(parallelism.isPresent());
    }

    @Test
    public void testSinkProviderWithInvalidParallelism() {
        assertThrows(
                IllegalStateException.class,
                () -> new AbstractDynamicTableSink.SinkProvider(() -> null, 0));
        assertThrows(
                IllegalStateException.class,
                () -> new AbstractDynamicTableSink.SinkProvider(() -> null, -1));
    }
}
