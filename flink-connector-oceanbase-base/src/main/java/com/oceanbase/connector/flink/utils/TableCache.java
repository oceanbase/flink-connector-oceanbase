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

package com.oceanbase.connector.flink.utils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class TableCache<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient Map<String, T> cache;

    private Map<String, T> getCache() {
        if (cache == null) {
            cache = new ConcurrentHashMap<>();
        }
        return cache;
    }

    public Collection<T> getAll() {
        return getCache().values();
    }

    public T get(String tableId, Supplier<T> supplier) {
        if (getCache().containsKey(tableId)) {
            return getCache().get(tableId);
        }
        T t = supplier.get();
        getCache().put(tableId, t);
        return t;
    }

    public void remove(String tableId) {
        getCache().remove(tableId);
    }

    public void clear() {
        if (cache != null) {
            cache.clear();
        }
    }
}
