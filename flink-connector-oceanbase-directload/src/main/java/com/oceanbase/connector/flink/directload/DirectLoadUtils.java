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

package com.oceanbase.connector.flink.directload;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;

/** The utils of {@link DirectLoader} */
public class DirectLoadUtils {

    public static DirectLoader buildDirectLoaderFromConnOption(
            OBDirectLoadConnectorOptions connectorOptions) {
        try {
            return new DirectLoaderBuilder()
                    .host(connectorOptions.getDirectLoadHost())
                    .port(connectorOptions.getDirectLoadPort())
                    .user(connectorOptions.getUsername())
                    .password(connectorOptions.getPassword())
                    .tenant(connectorOptions.getTenantName())
                    .schema(connectorOptions.getSchemaName())
                    .table(connectorOptions.getTableName())
                    .enableMultiNodeWrite(connectorOptions.getEnableMultiNodeWrite())
                    .duplicateKeyAction(connectorOptions.getDirectLoadDupAction())
                    .maxErrorCount(connectorOptions.getDirectLoadMaxErrorRows())
                    .timeout(connectorOptions.getDirectLoadTimeout())
                    .heartBeatTimeout(connectorOptions.getDirectLoadHeartbeatTimeout())
                    .heartBeatInterval(connectorOptions.getDirectLoadHeartbeatInterval())
                    .directLoadMethod(connectorOptions.getDirectLoadLoadMethod())
                    .parallel(connectorOptions.getDirectLoadParallel())
                    .executionId(connectorOptions.getExecutionId())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Fail to build DirectLoader.", e);
        }
    }
}
