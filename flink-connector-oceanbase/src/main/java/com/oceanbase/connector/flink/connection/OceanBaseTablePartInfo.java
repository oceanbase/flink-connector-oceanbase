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

import com.oceanbase.partition.calculator.ObPartIdCalculator;
import com.oceanbase.partition.calculator.model.TableEntry;
import com.oceanbase.partition.metadata.desc.ObPartColumn;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OceanBaseTablePartInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ObPartIdCalculator partIdCalculator;
    private final List<String> partColumnNames;
    private final Map<String, Integer> partColumnIndexMap;

    public OceanBaseTablePartInfo(TableEntry tableEntry, boolean isV4) {
        this.partIdCalculator = new ObPartIdCalculator(false, tableEntry, isV4);
        List<ObPartColumn> partColumns = tableEntry.getTablePart().getPartColumns();
        this.partColumnNames =
                partColumns.stream().map(ObPartColumn::getColumnName).collect(Collectors.toList());
        this.partColumnIndexMap =
                tableEntry.getTablePart().getPartColumns().stream()
                        .collect(
                                Collectors.toMap(
                                        ObPartColumn::getColumnName, ObPartColumn::getColumnIndex));
    }

    public ObPartIdCalculator getPartIdCalculator() {
        return partIdCalculator;
    }

    public List<String> getPartColumnNames() {
        return partColumnNames;
    }

    public Map<String, Integer> getPartColumnIndexMap() {
        return partColumnIndexMap;
    }
}
