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

import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import org.apache.cstore.dictionary.DictionaryValueEncoding;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.collect.Iterables.getOnlyElement;

public class CStorePlugin
        implements Plugin
{
    private final String name;
    private final Module metadataModule;
    private final Map<String, Module> fileSystemProviders;
    private final Map<String, Module> backupProviders;

    public CStorePlugin()
    {
        this(getPluginInfo());
    }

    public CStorePlugin(PluginInfo pluginInfo)
    {
        this.name = pluginInfo.getName();
        this.metadataModule = pluginInfo.getMetadataModule();
        this.fileSystemProviders = pluginInfo.getFileSystemProviders();
        this.backupProviders = pluginInfo.getBackupProviders();
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new CStoreConnectorFactory(name, metadataModule));
    }

    @Override
    public Iterable<BlockEncoding> getBlockEncodings()
    {
        return ImmutableList.of(new DictionaryValueEncoding());
    }

    private static PluginInfo getPluginInfo()
    {
        ClassLoader classLoader = CStorePlugin.class.getClassLoader();
        ServiceLoader<PluginInfo> loader = ServiceLoader.load(PluginInfo.class, classLoader);
        List<PluginInfo> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new PluginInfo() : getOnlyElement(list);
    }
}
