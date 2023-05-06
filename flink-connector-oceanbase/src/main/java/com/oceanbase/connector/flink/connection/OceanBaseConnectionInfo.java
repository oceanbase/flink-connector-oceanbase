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
import com.oceanbase.partition.calculator.enums.ObServerMode;
import com.oceanbase.partition.calculator.model.TableEntryKey;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class OceanBaseConnectionInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum CompatibleMode {
        MYSQL,
        ORACLE;

        public static CompatibleMode fromString(String text) {
            if (StringUtils.isBlank(text)) {
                throw new IllegalArgumentException("Compatible mode should not be blank");
            }
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

        public static Version fromString(String text) {
            return (StringUtils.isBlank(text) || !text.startsWith("4.")) ? LEGACY : V4;
        }

        public boolean isV4() {
            return Version.V4.equals(this);
        }
    }

    private final OceanBaseDialect dialect;
    private final Version version;
    private final String tableName;
    private final TableEntryKey tableEntryKey;

    public OceanBaseConnectionInfo(
            OceanBaseConnectionOptions options,
            CompatibleMode compatibleMode,
            OceanBaseDialect dialect,
            String version) {
        this.dialect = dialect;
        this.version = Version.fromString(version);
        String username = options.getUsername();
        String clusterName = "";
        String tenantName = "sys";
        try {
            if (username.contains("@")) {
                int i = username.indexOf("@");
                String s = username.substring(i + 1);
                String[] arr = s.split("#");
                if (arr.length > 0) {
                    tenantName = arr[0];
                }
                if (arr.length > 1) {
                    clusterName = arr[1];
                }
            } else {
                String[] arr = username.split(":");
                if (arr.length == 3) {
                    clusterName = arr[0];
                    tenantName = arr[1];
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse username", e);
        }
        this.tableName = options.getTableName();
        this.tableEntryKey =
                new TableEntryKey(
                        clusterName,
                        tenantName,
                        options.getSchemaName(),
                        options.getTableName(),
                        compatibleMode.isMySqlMode()
                                ? ObServerMode.fromMySql(version)
                                : ObServerMode.fromOracle(version));
    }

    public OceanBaseDialect getDialect() {
        return dialect;
    }

    public Version getVersion() {
        return version;
    }

    public String getTableName() {
        return tableName;
    }

    public TableEntryKey getTableEntryKey() {
        return tableEntryKey;
    }
}
