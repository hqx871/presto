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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import javax.inject.Inject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_FILE_SYSTEM_ERROR;

public class LocalCStoreDataEnvironment
        implements CStoreDataEnvironment
{
    private static final Configuration CONFIGURATION = new Configuration();
    private final RawLocalFileSystem localFileSystem;

    @Inject
    public LocalCStoreDataEnvironment()
    {
        try {
            this.localFileSystem = new CStoreLocalFileSystem(CONFIGURATION);
        }
        catch (IOException e) {
            throw new PrestoException(CSTORE_FILE_SYSTEM_ERROR, "Raptor cannot create local file system", e);
        }
    }

    @Override
    public RawLocalFileSystem getFileSystem(HdfsContext ignore)
    {
        return localFileSystem;
    }

    public RawLocalFileSystem getLocalFileSystem()
    {
        return localFileSystem;
    }

    @Override
    public DataSink createDataSink(FileSystem fileSystem, Path path)
            throws IOException
    {
        return new OutputStreamDataSink(new FileOutputStream(localFileSystem.pathToFile(path)));
    }

    public static Optional<RawLocalFileSystem> tryGetLocalFileSystem(CStoreDataEnvironment environment)
    {
        if (environment instanceof LocalCStoreDataEnvironment) {
            return Optional.of(((LocalCStoreDataEnvironment) environment).getLocalFileSystem());
        }
        return Optional.empty();
    }
}