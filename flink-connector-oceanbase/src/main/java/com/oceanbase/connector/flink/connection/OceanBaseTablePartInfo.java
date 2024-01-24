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
                            obPartColumn ->
                                    this.partColumnIndexMap.put(
                                            obPartColumn.getColumnName(),
                                            obPartColumn.getColumnIndex()));
        }
    }

    public ObPartIdCalculator getPartIdCalculator() {
        return partIdCalculator;
    }

    public Map<String, Integer> getPartColumnIndexMap() {
        return partColumnIndexMap;
    }
}
