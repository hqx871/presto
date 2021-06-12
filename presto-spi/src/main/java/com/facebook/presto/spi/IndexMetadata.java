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
package com.facebook.presto.spi;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class IndexMetadata
{
    private final String name;
    private final List<String> columns;
    private final Optional<String> using;

    public IndexMetadata(String name, List<String> columns, Optional<String> using)
    {
        this.name = name;
        this.columns = columns;
        this.using = using;
    }

    public String getName()
    {
        return name;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public Optional<String> getUsing()
    {
        return using;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexMetadata that = (IndexMetadata) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(using, that.using);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, columns, using);
    }
}
