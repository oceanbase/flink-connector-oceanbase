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

import com.oceanbase.connector.flink.sink.OBKVDirectLoadDynamicTableSink;
import com.oceanbase.connector.flink.utils.OptionUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** The factory of direct-load dynamic table sink. see {@link DynamicTableSinkFactory}. */
public class OBKVDirectLoadDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "obkv-directload";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        ResolvedSchema physicalSchema =
                new ResolvedSchema(
                        resolvedSchema.getColumns().stream()
                                .filter(Column::isPhysical)
                                .collect(Collectors.toList()),
                        resolvedSchema.getWatermarkSpecs(),
                        resolvedSchema.getPrimaryKey().orElse(null));
        Map<String, String> options = context.getCatalogTable().getOptions();
        OptionUtils.printOptions(IDENTIFIER, options);
        RuntimeExecutionMode runtimeExecutionMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE);
        int numberOfTaskSlots = context.getConfiguration().get(TaskManagerOptions.NUM_TASK_SLOTS);
        return new OBKVDirectLoadDynamicTableSink(
                physicalSchema,
                new OBKVDirectLoadConnectorOptions(options),
                runtimeExecutionMode,
                numberOfTaskSlots);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OBKVDirectLoadConnectorOptions.HOST);
        options.add(OBKVDirectLoadConnectorOptions.PORT);
        options.add(OBKVDirectLoadConnectorOptions.USERNAME);
        options.add(OBKVDirectLoadConnectorOptions.TENANT_NAME);
        options.add(OBKVDirectLoadConnectorOptions.PASSWORD);
        options.add(OBKVDirectLoadConnectorOptions.SCHEMA_NAME);
        options.add(OBKVDirectLoadConnectorOptions.TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OBKVDirectLoadConnectorOptions.EXECUTION_ID);
        options.add(OBKVDirectLoadConnectorOptions.PARALLEL);
        options.add(OBKVDirectLoadConnectorOptions.MAX_ERROR_ROWS);
        options.add(OBKVDirectLoadConnectorOptions.DUP_ACTION);
        options.add(OBKVDirectLoadConnectorOptions.TIMEOUT);
        options.add(OBKVDirectLoadConnectorOptions.HEARTBEAT_TIMEOUT);
        options.add(OBKVDirectLoadConnectorOptions.LOAD_METHOD);
        options.add(OBKVDirectLoadConnectorOptions.ENABLE_MULTI_NODE_WRITE);
        options.add(OBKVDirectLoadConnectorOptions.BUFFER_SIZE);
        return options;
    }
}
