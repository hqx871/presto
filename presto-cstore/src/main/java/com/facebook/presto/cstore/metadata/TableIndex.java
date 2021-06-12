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
package com.facebook.presto.cstore.metadata;

import com.facebook.presto.common.type.TypeManager;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TableIndex
{
    private final long indexId;
    private final long tableId;
    private final long[] columnIds;
    private final String indexType;

    public TableIndex(long indexId, long tableId, long[] columnIds, String indexType)
    {
        this.indexId = indexId;
        this.tableId = tableId;
        this.columnIds = columnIds;
        this.indexType = indexType;
    }

    public long getTableId()
    {
        return tableId;
    }

    public long[] getColumnIds()
    {
        return columnIds;
    }

    public String getIndexType()
    {
        return indexType;
    }

    public long getIndexId()
    {
        return indexId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("indexId", indexId)
                .add("tableId", tableId)
                .add("columnIds", columnIds)
                .add("indexType", indexType)
                .toString();
    }

    public static class Mapper
            implements ResultSetMapper<TableIndex>
    {
        private final TypeManager typeManager;

        @Inject
        public Mapper(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public TableIndex map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            //Type type = typeManager.getType(parseTypeSignature(typeName));
            return new TableIndex(
                    r.getLong("index_id"),
                    r.getLong("table_id"),
                    parseColumnIds(r.getString("column_ids")),
                    r.getString("index_type"));
        }
    }

    private static long[] parseColumnIds(String str)
    {
        return Arrays.stream(str.split(",")).mapToLong(Long::parseLong).toArray();
    }
}
