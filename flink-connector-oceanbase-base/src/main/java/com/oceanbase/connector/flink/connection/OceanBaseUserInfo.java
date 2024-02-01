/*
 * Copyright 2024 OceanBase.
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

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class OceanBaseUserInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String cluster;
    private String tenant;
    private final String user;

    public OceanBaseUserInfo(String cluster, String tenant, String user) {
        this.cluster = cluster;
        this.tenant = tenant;
        this.user = user;
    }

    public String getCluster() {
        return cluster;
    }

    public String getTenant() {
        return tenant;
    }

    public String getUser() {
        return user;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public static OceanBaseUserInfo parse(String username) {
        final String sepUserAtTenant = "@";
        final String sepTenantAtCluster = "#";
        final String sep = ":";
        final int expectedSepCount = 2;
        if (username.contains(sepTenantAtCluster) && username.contains(sepUserAtTenant)) {
            // user@tenant#cluster
            String[] parts = username.split(sepTenantAtCluster);
            String[] userAndTenant = parts[0].split(sepUserAtTenant);
            return new OceanBaseUserInfo(parts[1], userAndTenant[1], userAndTenant[0]);
        } else if (StringUtils.countMatches(username, sep) == expectedSepCount) {
            // cluster:tenant:user
            String[] parts = username.split(sep);
            return new OceanBaseUserInfo(parts[0], parts[1], parts[2]);
        } else if (username.contains(sepUserAtTenant) && !username.contains(sepTenantAtCluster)) {
            // user@tenant
            String[] parts = username.split(sepUserAtTenant);
            return new OceanBaseUserInfo(null, parts[1], parts[0]);
        } else {
            // only user
            return new OceanBaseUserInfo(null, null, username);
        }
    }
}
