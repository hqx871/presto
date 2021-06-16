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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_WRITER_DATA_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CStoreFileWriter
        implements FileWriter
{
    private final CStoreWriter cstoreWriter;

    private boolean closed;
    private long rowCount;
    private long uncompressedSize;

    public CStoreFileWriter(
            List<Long> columnIds,
            List<Type> columnTypes,
            File stagingDirectory,
            UUID shardUuid,
            DataSink target)
    {
        checkArgument(requireNonNull(columnIds, "columnIds is null").size() == requireNonNull(columnTypes, "columnTypes is null").size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        List<String> columnNames = columnIds.stream().map(Object::toString).collect(toImmutableList());

        cstoreWriter = new CStoreWriter(columnIds, stagingDirectory, target, columnNames, columnTypes, shardUuid);
    }

    public void setup()
    {
        cstoreWriter.setup();
    }

    @Override
    public void appendPages(List<Page> pages)
    {
        for (Page page : pages) {
            try {
                cstoreWriter.write(page);
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(CSTORE_WRITER_DATA_ERROR, e);
            }
            uncompressedSize += page.getLogicalSizeInBytes();
            rowCount += page.getPositionCount();
        }
    }

    @Override
    public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");
        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = inputPages.get(pageIndexes[i]);
            // This will do data copy; be aware
            Page singleValuePage = page.getSingleValuePage(positionIndexes[i]);
            try {
                cstoreWriter.write(singleValuePage);
                uncompressedSize += singleValuePage.getLogicalSizeInBytes();
                rowCount++;
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(CSTORE_WRITER_DATA_ERROR, e);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        cstoreWriter.close();
    }

    @Override
    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    private static <T> boolean isUnique(Collection<T> items)
    {
        return new HashSet<>(items).size() == items.size();
    }
}
