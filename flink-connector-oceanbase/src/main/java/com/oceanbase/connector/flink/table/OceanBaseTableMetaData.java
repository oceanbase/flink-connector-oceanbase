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

import java.io.Serializable;

public class OceanBaseTableMetaData implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String[] columnNames;
    private final int[] columnTypes;
    private final String[] columnTypeNames;
    private final int[] columnPrecision;
    private final int[] columnScales;

    public OceanBaseTableMetaData(
            String[] columnNames,
            int[] columnTypes,
            String[] columnTypeNames,
            int[] columnPrecision,
            int[] columnScales) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnTypeNames = columnTypeNames;
        this.columnPrecision = columnPrecision;
        this.columnScales = columnScales;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public int[] getColumnTypes() {
        return columnTypes;
    }

    public String[] getColumnTypeNames() {
        return columnTypeNames;
    }

    public int[] getColumnPrecision() {
        return columnPrecision;
    }

    public int[] getColumnScales() {
        return columnScales;
    }
}
