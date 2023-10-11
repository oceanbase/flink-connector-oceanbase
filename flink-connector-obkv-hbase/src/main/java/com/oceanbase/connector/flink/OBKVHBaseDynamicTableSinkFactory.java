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

package com.oceanbase.connector.flink;

import com.oceanbase.connector.flink.sink.OBKVHBaseDynamicTableSink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class OBKVHBaseDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "obkv-hbase";

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
        OBKVHBaseConnectorOptions options =
                new OBKVHBaseConnectorOptions(context.getCatalogTable().getOptions());
        return new OBKVHBaseDynamicTableSink(physicalSchema, options);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OBKVHBaseConnectorOptions.URL);
        options.add(OBKVHBaseConnectorOptions.TABLE_NAME);
        options.add(OBKVHBaseConnectorOptions.USERNAME);
        options.add(OBKVHBaseConnectorOptions.PASSWORD);
        options.add(OBKVHBaseConnectorOptions.SYS_USERNAME);
        options.add(OBKVHBaseConnectorOptions.SYS_PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OBKVHBaseConnectorOptions.BUFFER_FLUSH_INTERVAL);
        options.add(OBKVHBaseConnectorOptions.BUFFER_SIZE);
        options.add(OBKVHBaseConnectorOptions.BUFFER_BATCH_SIZE);
        options.add(OBKVHBaseConnectorOptions.MAX_RETRIES);
        return options;
    }
}
