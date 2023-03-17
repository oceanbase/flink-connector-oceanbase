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

package com.oceanbase.connector.flink.dialect;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.stream.Collectors;

public class OceanBaseOracleDialect implements OceanBaseDialect {

    @Override
    public String quoteIdentifier(@Nonnull String identifier) {
        return identifier;
    }

    @Override
    public String getUpsertStatement(
            @Nonnull String tableName,
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields) {
        String sourceFields =
                fieldNames.stream()
                        .map(f -> "? AS " + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String onClause =
                uniqueKeyFields.stream()
                        .map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
                        .collect(Collectors.joining(" and "));

        String updateClause =
                fieldNames.stream()
                        .filter(f -> !uniqueKeyFields.contains(f))
                        .map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String insertFields =
                fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));

        String valuesClause =
                fieldNames.stream()
                        .map(f -> "s." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        return "MERGE INTO "
                + tableName
                + " t "
                + " USING (SELECT "
                + sourceFields
                + " FROM DUAL) s "
                + " ON ("
                + onClause
                + ") "
                + " WHEN MATCHED THEN UPDATE SET "
                + updateClause
                + " WHEN NOT MATCHED THEN INSERT ("
                + insertFields
                + ")"
                + " VALUES ("
                + valuesClause
                + ")";
    }
}
