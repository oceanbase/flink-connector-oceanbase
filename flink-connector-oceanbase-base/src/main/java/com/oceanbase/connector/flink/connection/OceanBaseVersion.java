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

import javax.annotation.Nonnull;

import java.io.Serializable;

public class OceanBaseVersion implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String version;

    OceanBaseVersion(@Nonnull String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public boolean isV4() {
        return version.startsWith("4.");
    }

    public static OceanBaseVersion fromVersionComment(String versionComment) {
        if (StringUtils.isBlank(versionComment)) {
            throw new RuntimeException("Version comment must not be empty");
        }
        String[] parts = versionComment.split(" ");
        if (parts.length <= 1) {
            throw new RuntimeException("Invalid version comment: " + versionComment);
        }
        return new OceanBaseVersion(parts[1]);
    }
}
