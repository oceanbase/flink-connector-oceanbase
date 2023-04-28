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

package com.oceanbase.connector.flink.table;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OceanBaseTableSchema implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<String> fieldNames;
    private final RowData.FieldGetter[] fieldGetters;
    private final boolean hasKey;
    private final List<String> keyFieldNames;
    private final RowData.FieldGetter[] keyFieldGetters;
    private final RowData.FieldGetter[] nonKeyFieldGetters;
    private final RowData.FieldGetter[] partitionFieldGetters;

    public OceanBaseTableSchema(
            ResolvedSchema schema, OceanBaseTableMetaData metaData, List<String> partitionColumns) {
        this.fieldNames = Arrays.asList(metaData.getColumnNames());
        this.hasKey = schema.getPrimaryKey().isPresent();
        this.keyFieldNames =
                schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(new ArrayList<>());
        this.fieldGetters = new RowData.FieldGetter[fieldNames.size()];
        this.keyFieldGetters = new RowData.FieldGetter[fieldNames.size()];
        this.nonKeyFieldGetters = new RowData.FieldGetter[fieldNames.size() - keyFieldNames.size()];
        this.partitionFieldGetters = new RowData.FieldGetter[fieldNames.size()];

        int k = 0, n = 0;
        List<String> columnNames = schema.getColumnNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = this.fieldNames.get(i);
            int index = columnNames.indexOf(fieldName);
            if (index < 0) {
                throw new RuntimeException(
                        "JDBC column " + fieldName + " not found in Flink table");
            }
            RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(
                            schema.getColumnDataTypes().get(index).getLogicalType(), index);
            this.fieldGetters[i] = fieldGetter;
            if (this.keyFieldNames.contains(fieldName)) {
                this.keyFieldGetters[k++] = fieldGetter;
            } else {
                this.nonKeyFieldGetters[n++] = fieldGetter;
            }
            if (partitionColumns.contains(fieldName)) {
                this.partitionFieldGetters[i] = fieldGetter;
            } else {
                this.partitionFieldGetters[i] = row -> null;
            }
        }
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public RowData.FieldGetter[] getFieldGetters() {
        return fieldGetters;
    }

    public boolean isHasKey() {
        return hasKey;
    }

    public List<String> getKeyFieldNames() {
        return keyFieldNames;
    }

    public RowData.FieldGetter[] getKeyFieldGetters() {
        return keyFieldGetters;
    }

    public RowData.FieldGetter[] getNonKeyFieldGetters() {
        return nonKeyFieldGetters;
    }

    public RowData.FieldGetter[] getPartitionFieldGetters() {
        return partitionFieldGetters;
    }
}
