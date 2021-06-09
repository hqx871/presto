/*
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
package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class CStoreOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final CStoreTableHandle table;
    private final List<CStoreColumnHandle> columns;

    @JsonCreator
    public CStoreOutputTableHandle(
            @JsonProperty("table") CStoreTableHandle table,
            @JsonProperty("columns") List<CStoreColumnHandle> columns)
    {
        this.table = requireNonNull(table, "table is null");
        this.columns = columns;
    }

    @JsonProperty
    public CStoreTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public List<CStoreColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("columns", columns)
                .toString();
    }
}
