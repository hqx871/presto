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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AddIndex
        extends Statement
{
    private final QualifiedName table;
    private final IndexDefinition index;
    private final boolean tableExists;
    private final boolean indexNotExists;

    public AddIndex(Optional<NodeLocation> location, QualifiedName table, IndexDefinition index, boolean tableExists, boolean indexNotExists)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.index = requireNonNull(index, "column is null");
        this.tableExists = tableExists;
        this.indexNotExists = indexNotExists;
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public IndexDefinition getIndex()
    {
        return index;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public boolean isIndexNotExists()
    {
        return indexNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddIndex(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(index);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, index);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AddIndex o = (AddIndex) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(index, o.index);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", table)
                .add("column", index)
                .toString();
    }
}
