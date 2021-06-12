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

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class CStoreIndexProvider
        implements ConnectorIndexProvider
{
    @Override
    public ConnectorIndex getIndex(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorIndexHandle indexHandle,
            List<ColumnHandle> lookupSchema,
            List<ColumnHandle> outputSchema)
    {
        CStoreIndexHandle tpchIndexHandle = (CStoreIndexHandle) indexHandle;
        Map<ColumnHandle, NullableValue> fixedValues = TupleDomain.extractFixedValues(tpchIndexHandle.getTupleDomain()).orElse(ImmutableMap.of())
                .entrySet().stream()
                //.filter(entry -> !indexableColumns.contains(entry.getKey()))
                .filter(entry -> !entry.getValue().isNull()) // strip nulls since meaningless in index join lookups
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        checkArgument(lookupSchema.stream().noneMatch(handle -> fixedValues.keySet().contains(handle)),
                "Lookup columnHandles are not expected to overlap with the fixed value predicates");

        // Establish an order for the fixedValues
        List<ColumnHandle> fixedValueColumns = ImmutableList.copyOf(fixedValues.keySet());

        // Extract the fixedValues as their raw values and types
        List<Object> rawFixedValues = new ArrayList<>(fixedValueColumns.size());
        List<Type> rawFixedTypes = new ArrayList<>(fixedValueColumns.size());
        for (ColumnHandle fixedValueColumn : fixedValueColumns) {
            rawFixedValues.add(fixedValues.get(fixedValueColumn).getValue());
            rawFixedTypes.add(((CStoreColumnHandle) fixedValueColumn).getColumnType());
        }

        // Establish the schema after we append the fixed values to the lookup keys.
        List<ColumnHandle> finalLookupSchema = ImmutableList.<ColumnHandle>builder()
                .addAll(lookupSchema)
                .addAll(fixedValueColumns)
                .build();

        return new CStoreConnectorIndex();
    }

    private static List<Integer> computeRemap(List<String> startSchema, List<String> endSchema)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (String columnName : endSchema) {
            int index = startSchema.indexOf(columnName);
            checkArgument(index != -1, "Column name in end that is not in the start: %s", columnName);
            builder.add(index);
        }
        return builder.build();
    }

    public static List<String> handleToNames(List<ColumnHandle> columnHandles)
    {
        return columnHandles.stream()
                .map(CStoreColumnHandle.class::cast)
                .map(CStoreColumnHandle::getColumnName)
                .collect(toList());
    }
}
