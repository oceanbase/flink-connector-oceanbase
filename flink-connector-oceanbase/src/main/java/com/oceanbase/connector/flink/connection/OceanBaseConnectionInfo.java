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

import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.partition.calculator.model.TableEntryKey;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;

public class OceanBaseConnectionInfo {

    public enum CompatibleMode {
        MYSQL,
        ORACLE;

        public static CompatibleMode fromString(@Nonnull String text) {
            switch (text.trim().toUpperCase()) {
                case "MYSQL":
                    return MYSQL;
                case "ORACLE":
                    return ORACLE;
                default:
                    throw new UnsupportedOperationException("Unsupported compatible mode: " + text);
            }
        }

        public boolean isMySqlMode() {
            return CompatibleMode.MYSQL.equals(this);
        }
    }

    public enum Version {
        LEGACY,
        V4;

        public static Version fromString(@Nonnull String text) {
            return (StringUtils.isBlank(text) || !text.startsWith("4.")) ? LEGACY : V4;
        }

        public boolean isV4() {
            return Version.V4.equals(this);
        }
    }

    private final OceanBaseDialect dialect;
    private final Version version;
    private final TableEntryKey tableEntryKey;

    public OceanBaseConnectionInfo(
            OceanBaseDialect dialect, Version version, TableEntryKey tableEntryKey) {
        this.dialect = dialect;
        this.version = version;
        this.tableEntryKey = tableEntryKey;
    }

    public OceanBaseDialect getDialect() {
        return dialect;
    }

    public Version getVersion() {
        return version;
    }

    public TableEntryKey getTableEntryKey() {
        return tableEntryKey;
    }
}
