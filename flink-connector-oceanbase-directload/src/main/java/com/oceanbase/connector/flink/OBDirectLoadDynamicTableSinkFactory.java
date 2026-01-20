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

import com.oceanbase.connector.flink.sink.OBDirectLoadDynamicTableSink;
import com.oceanbase.connector.flink.utils.OptionUtils;

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
public class OBDirectLoadDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "oceanbase-directload";

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
        context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE);
        int numberOfTaskSlots = context.getConfiguration().get(TaskManagerOptions.NUM_TASK_SLOTS);
        return new OBDirectLoadDynamicTableSink(
                physicalSchema, new OBDirectLoadConnectorOptions(options), numberOfTaskSlots);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OBDirectLoadConnectorOptions.HOST);
        options.add(OBDirectLoadConnectorOptions.PORT);
        options.add(OBDirectLoadConnectorOptions.USERNAME);
        options.add(OBDirectLoadConnectorOptions.TENANT_NAME);
        options.add(OBDirectLoadConnectorOptions.PASSWORD);
        options.add(OBDirectLoadConnectorOptions.SCHEMA_NAME);
        options.add(OBDirectLoadConnectorOptions.TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OBDirectLoadConnectorOptions.PARALLEL);
        options.add(OBDirectLoadConnectorOptions.MAX_ERROR_ROWS);
        options.add(OBDirectLoadConnectorOptions.DUP_ACTION);
        options.add(OBDirectLoadConnectorOptions.TIMEOUT);
        options.add(OBDirectLoadConnectorOptions.HEARTBEAT_TIMEOUT);
        options.add(OBDirectLoadConnectorOptions.LOAD_METHOD);
        options.add(OBDirectLoadConnectorOptions.BUFFER_SIZE);
        return options;
    }
}
