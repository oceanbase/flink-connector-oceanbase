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

package com.oceanbase.connector.flink;

import org.apache.flink.configuration.ConfigOption;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link OBDirectLoadDynamicTableSinkFactory}. */
public class OBDirectLoadDynamicTableSinkFactoryTest {

    @Test
    public void testOptionalOptionsAreFullyRegistered() {
        Set<ConfigOption<?>> expected =
                new HashSet<>(
                        Arrays.asList(
                                OBDirectLoadConnectorOptions.PARALLEL,
                                OBDirectLoadConnectorOptions.WRITE_CONNECTION_NUM,
                                OBDirectLoadConnectorOptions.MAX_ERROR_ROWS,
                                OBDirectLoadConnectorOptions.DUP_ACTION,
                                OBDirectLoadConnectorOptions.TIMEOUT,
                                OBDirectLoadConnectorOptions.HEARTBEAT_TIMEOUT,
                                OBDirectLoadConnectorOptions.HEARTBEAT_INTERVAL,
                                OBDirectLoadConnectorOptions.LOAD_METHOD,
                                OBDirectLoadConnectorOptions.BUFFER_SIZE,
                                OBDirectLoadConnectorOptions.BOUNDED_MODE_ENABLED));

        assertEquals(expected, new OBDirectLoadDynamicTableSinkFactory().optionalOptions());
    }
}
