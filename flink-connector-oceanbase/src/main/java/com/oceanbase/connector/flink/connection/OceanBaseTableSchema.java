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

package com.oceanbase.connector.flink.connection;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;

import com.oceanbase.connector.flink.converter.OceanBaseRowConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class OceanBaseTableSchema implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<String> fieldNames;
    private final RowData.FieldGetter[] fieldGetters;
    private final boolean hasKey;
    private final List<String> keyFieldNames;
    private final RowData.FieldGetter[] keyFieldGetters;
    private final RowData.FieldGetter[] nonKeyFieldGetters;

    public OceanBaseTableSchema(ResolvedSchema schema) {
        this.fieldNames = schema.getColumnNames();
        this.fieldGetters = new RowData.FieldGetter[this.fieldNames.size()];
        this.hasKey = schema.getPrimaryKey().isPresent();
        this.keyFieldNames =
                schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(new ArrayList<>());
        this.keyFieldGetters = new RowData.FieldGetter[this.keyFieldNames.size()];
        this.nonKeyFieldGetters =
                new RowData.FieldGetter[this.fieldNames.size() - this.keyFieldNames.size()];
        int k = 0, n = 0;
        OceanBaseRowConverter rowConverter = new OceanBaseRowConverter();
        for (int i = 0; i < this.fieldNames.size(); i++) {
            RowData.FieldGetter fieldGetter =
                    rowConverter.createFieldGetter(
                            schema.getColumnDataTypes().get(i).getLogicalType(), i);
            this.fieldGetters[i] = fieldGetter;
            if (this.keyFieldNames.contains(this.fieldNames.get(i))) {
                this.keyFieldGetters[k++] = fieldGetter;
            } else {
                this.nonKeyFieldGetters[n++] = fieldGetter;
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
}
