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

package com.oceanbase.connector.flink.dialect;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.stream.Collectors;

public class OceanBaseOracleDialect implements OceanBaseDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public String quoteIdentifier(@Nonnull String identifier) {
        return identifier;
    }

    @Override
    public String getUpsertStatement(
            @Nonnull String schemaName,
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
                + getFullTableName(schemaName, tableName)
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

    @Override
    public String getSysDatabase() {
        return "SYS";
    }

    @Override
    public String getSelectOBVersionStatement() {
        return "SELECT OB_VERSION() FROM DUAL";
    }
}
