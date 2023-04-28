/*
 * Copyright (c) 2023 OceanBase
 * flink-connector-oceanbase is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.oceanbase.connector.flink.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.oceanbase.connector.flink.table.OceanBaseConnectorOptions;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class OceanBaseDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "oceanbase";

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
        OceanBaseConnectorOptions options =
                new OceanBaseConnectorOptions(context.getCatalogTable().getOptions());
        return new OceanBaseDynamicTableSink(physicalSchema, options);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OceanBaseConnectorOptions.URL);
        options.add(OceanBaseConnectorOptions.SCHEMA_NAME);
        options.add(OceanBaseConnectorOptions.TABLE_NAME);
        options.add(OceanBaseConnectorOptions.USERNAME);
        options.add(OceanBaseConnectorOptions.PASSWORD);
        options.add(OceanBaseConnectorOptions.DRIVER_CLASS_NAME);
        options.add(OceanBaseConnectorOptions.CONNECTION_POOL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OceanBaseConnectorOptions.CONNECTION_POOL_PROPERTIES);
        options.add(OceanBaseConnectorOptions.UPSERT_MODE);
        options.add(OceanBaseConnectorOptions.BUFFER_FLUSH_INTERVAL);
        options.add(OceanBaseConnectorOptions.BUFFER_SIZE);
        options.add(OceanBaseConnectorOptions.BUFFER_BATCH_SIZE);
        options.add(OceanBaseConnectorOptions.MAX_RETRIES);
        options.add(OceanBaseConnectorOptions.MEMSTORE_CHECK_ENABLED);
        options.add(OceanBaseConnectorOptions.MEMSTORE_THRESHOLD);
        options.add(OceanBaseConnectorOptions.MEMSTORE_CHECK_INTERVAL);
        options.add(OceanBaseConnectorOptions.PARTITION_KEY);
        return options;
    }
}
