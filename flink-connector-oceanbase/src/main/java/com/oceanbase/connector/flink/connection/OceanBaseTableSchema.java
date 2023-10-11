/*
 * Copyright (c) 2023 OceanBase.
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

package com.oceanbase.connector.flink.connection;

import com.oceanbase.connector.flink.converter.OceanBaseRowConverter;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;

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
