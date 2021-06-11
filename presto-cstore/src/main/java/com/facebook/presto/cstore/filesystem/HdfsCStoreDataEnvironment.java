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
package com.facebook.presto.cstore.filesystem;

import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.cstore.storage.CStoreDataEnvironment;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_FILE_SYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class HdfsCStoreDataEnvironment
        implements CStoreDataEnvironment
{
    private final Path baseLocation;
    private final CStoreHdfsConfiguration configuration;

    @Inject
    public HdfsCStoreDataEnvironment(Path baseLocation, CStoreHdfsConfiguration configuration)
    {
        this.baseLocation = requireNonNull(baseLocation, "baseLocation is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
    }

    @Override
    public FileSystem getFileSystem(HdfsContext hdfsContext)
    {
        try {
            return baseLocation.getFileSystem(configuration.getConfiguration(hdfsContext, baseLocation.toUri()));
        }
        catch (IOException e) {
            throw new PrestoException(CSTORE_FILE_SYSTEM_ERROR, "Raptor cannot create HDFS file system", e);
        }
    }

    @Override
    public DataSink createDataSink(FileSystem fileSystem, Path path)
            throws IOException
    {
        return new OutputStreamDataSink(fileSystem.create(path));
    }
}
