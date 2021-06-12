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
package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.metadata.ColumnStats;
import github.cstore.column.ByteCursor;
import github.cstore.column.CStoreColumnReader;
import github.cstore.column.DoubleCursor;
import github.cstore.column.IntCursor;
import github.cstore.column.LongCursor;
import github.cstore.column.StringCursor;
import github.cstore.column.StringEncodedColumnReader;
import github.cstore.column.VectorCursor;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

public final class ShardStats
{
    /**
     * Maximum length of a binary value stored in an index.
     */
    public static final int MAX_BINARY_INDEX_SIZE = 100;

    private ShardStats() {}

    public static Slice truncateIndexValue(Slice slice)
    {
        if (slice.length() > MAX_BINARY_INDEX_SIZE) {
            return slice.slice(0, MAX_BINARY_INDEX_SIZE);
        }
        return slice;
    }

    public static Optional<ColumnStats> computeColumnStats(CStoreColumnReader columnReader, long columnId, Type type)
            throws IOException
    {
        return Optional.ofNullable(doComputeColumnStats(columnReader, columnId, type));
    }

    private static ColumnStats doComputeColumnStats(CStoreColumnReader reader, long columnId, Type type)
            throws IOException
    {
        if (type.equals(BOOLEAN)) {
            return indexBoolean(reader, columnId);
        }
        if (type.equals(DateType.DATE) ||
                type.equals(TimeType.TIME)) {
            return indexInteger(type, reader, columnId);
        }
        if (type.equals(BigintType.BIGINT) ||
                type.equals(TimestampType.TIMESTAMP)) {
            return indexLong(type, reader, columnId);
        }
        if (type.equals(DOUBLE)) {
            return indexDouble(reader, columnId);
        }
        if (type instanceof VarcharType) {
            return indexString(type, (StringEncodedColumnReader) reader, columnId);
        }
        return null;
    }

    private static ColumnStats indexBoolean(CStoreColumnReader columnReader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        boolean min = false;
        boolean max = false;

        int vectorSize = 1024;
        int readOffset = 0;
        VectorCursor cursor = new ByteCursor(new int[vectorSize]);
        while (true) {
            int readSize = Math.min(vectorSize, columnReader.getRowCount() - readOffset);
            int batchSize = columnReader.read(readOffset, readSize, cursor);
            if (batchSize <= 0) {
                break;
            }
            Block block = cursor.toBlock(batchSize);
            readOffset += batchSize;

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                boolean value = BOOLEAN.getBoolean(block, i);
                if (!minSet || Boolean.compare(value, min) < 0) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || Boolean.compare(value, max) > 0) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexInteger(Type type, CStoreColumnReader columnReader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        long min = 0;
        long max = 0;

        int vectorSize = 1024;
        VectorCursor cursor = new IntCursor(new int[vectorSize]);
        int offset = 0;
        while (true) {
            int readSize = Math.min(vectorSize, columnReader.getRowCount() - offset);
            int batchSize = columnReader.read(offset, readSize, cursor);
            if (batchSize <= 0) {
                break;
            }
            Block block = cursor.toBlock(batchSize);
            offset += batchSize;

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                long value = type.getLong(block, i);
                if (!minSet || (value < min)) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || (value > max)) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexLong(Type type, CStoreColumnReader columnReader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        long min = 0;
        long max = 0;

        int vectorSize = 1024;
        VectorCursor cursor = new LongCursor(new long[vectorSize]);
        int offset = 0;
        while (true) {
            int readSize = Math.min(vectorSize, columnReader.getRowCount() - offset);
            int batchSize = columnReader.read(offset, readSize, cursor);
            if (batchSize <= 0) {
                break;
            }
            Block block = cursor.toBlock(batchSize);
            offset += batchSize;

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                long value = type.getLong(block, i);
                if (!minSet || (value < min)) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || (value > max)) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexDouble(CStoreColumnReader columnReader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        double min = 0;
        double max = 0;

        int vectorSize = 1024;
        VectorCursor cursor = new DoubleCursor(new long[vectorSize]);
        int readOffset = 0;
        while (true) {
            int readSize = Math.min(vectorSize, columnReader.getRowCount() - readOffset);
            int batchSize = columnReader.read(readOffset, readSize, cursor);
            if (batchSize <= 0) {
                break;
            }
            Block block = cursor.toBlock(batchSize);
            readOffset += batchSize;

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                double value = DOUBLE.getDouble(block, i);
                if (isNaN(value)) {
                    continue;
                }
                if (value == -0.0) {
                    value = 0.0;
                }
                if (!minSet || (value < min)) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || (value > max)) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        if (isInfinite(min)) {
            minSet = false;
        }
        if (isInfinite(max)) {
            maxSet = false;
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexString(Type type, StringEncodedColumnReader columnReader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        Slice min = null;
        Slice max = null;
        int vectorSize = 1024;
        VectorCursor cursor = new StringCursor(new int[vectorSize], columnReader.getDictionaryValue());
        int readOffset = 0;
        while (true) {
            int readSize = Math.min(vectorSize, columnReader.getRowCount() - readOffset);
            int batchSize = columnReader.read(readOffset, readSize, cursor);
            if (batchSize <= 0) {
                break;
            }
            Block block = cursor.toBlock(batchSize);
            readOffset += batchSize;

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                Slice slice = type.getSlice(block, i);
                slice = truncateIndexValue(slice);
                if (!minSet || (slice.compareTo(min) < 0)) {
                    minSet = true;
                    min = slice;
                }
                if (!maxSet || (slice.compareTo(max) > 0)) {
                    maxSet = true;
                    max = slice;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min.toStringUtf8() : null,
                maxSet ? max.toStringUtf8() : null);
    }
}
