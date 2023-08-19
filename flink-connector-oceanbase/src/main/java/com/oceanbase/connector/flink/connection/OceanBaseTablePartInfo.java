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
import com.oceanbase.partition.metadata.desc.ObTablePart;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class OceanBaseTablePartInfo {

    private final ObPartIdCalculator partIdCalculator;
    private final Map<String, Integer> partColumnIndexMap;

    public OceanBaseTablePartInfo(TableEntry tableEntry, boolean isV4) {
        this.partIdCalculator = new ObPartIdCalculator(false, tableEntry, isV4);
        this.partColumnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ObTablePart tablePart = tableEntry.getTablePart();
        if (tablePart != null) {
            Stream.concat(
                            tablePart.getPartColumns().stream(),
                            tablePart.getSubPartColumns().stream())
                    .forEach(
                            obPartColumn -> {
                                this.partColumnIndexMap.put(
                                        obPartColumn.getColumnName(),
                                        obPartColumn.getColumnIndex());
                            });
        }
    }

    public ObPartIdCalculator getPartIdCalculator() {
        return partIdCalculator;
    }

    public Map<String, Integer> getPartColumnIndexMap() {
        return partColumnIndexMap;
    }
}
