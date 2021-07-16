package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.WalMetadata;
import com.facebook.presto.cstore.storage.organization.WalCompactionManager;
import com.facebook.presto.spi.NodeManager;
import com.google.inject.Inject;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class WriteAheadLogManager
{
    private final String nodeId;
    private final File stagingDirectory;
    private final MetadataDao metadataDao;
    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final PagesSerdeFactory pagesSerdeFactory;
    private final WalCompactionManager walCompactionManager;

    @Inject
    public WriteAheadLogManager(StorageManagerConfig config,
            NodeManager nodeManager,
            MetadataDao metadataDao,
            BlockEncodingSerde blockEncodingSerde,
            ShardManager shardManager,
            WalCompactionManager walCompactionManager)
    {
        this.nodeId = requireNonNull(nodeManager.getCurrentNode().getNodeIdentifier(), "nodeId is null");
        this.stagingDirectory = new File(config.getStagingDirectory());
        this.metadataDao = metadataDao;
        this.nodeManager = nodeManager;
        this.shardManager = shardManager;
        this.pagesSerdeFactory = new PagesSerdeFactory(blockEncodingSerde, true, true);
        this.walCompactionManager = walCompactionManager;
    }

    public WriteAheadLogAppender getScrollAppender(long tableId, long transactionId)
    {
        //todo cache by table id
        return new WriteAheadLogScrollAppender(tableId, this, transactionId);
    }

    public WriteAheadLogFileAppender createFileAppender(long transactionId, UUID walUuid)
    {
        metadataDao.insertWal(nodeId, walUuid, transactionId);
        URI uri = Paths.get(stagingDirectory.getAbsolutePath(), walUuid.toString()).toUri();
        try {
            File file = new File(uri);
            if (!file.exists() || !file.isFile()) {
                file.createNewFile();
            }
            SliceOutput output = new OutputStreamSliceOutput(new FileOutputStream(file));
            return new WriteAheadLogFileAppender(walUuid, 64 << 20, output);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void commit(UUID walUuid)
    {
        walCompactionManager.commit(walUuid);
    }

    public void compact(UUID walUuid, long baseOffset, long transactionId)
    {
        metadataDao.updateWalBaseOffset(walUuid, baseOffset, transactionId);
    }

    public Iterable<WriteAheadLogReader> listReader()
    {
        List<WalMetadata> wals = metadataDao.listWal(nodeId);
        return wals.stream()
                .map(walMetadata -> getReader(walMetadata))
                .collect(Collectors.toList());
    }

    public WriteAheadLogReader getReader(WalMetadata walMetadata)
    {
        URI uri = Paths.get(stagingDirectory.getAbsolutePath(), walMetadata.getWalUuid().toString()).toUri();
        File file = new File(uri);
        if (!file.exists() || !file.isFile()) {
            throw new IllegalStateException();
        }
        SliceInput sliceInput;
        try {
            sliceInput = new InputStreamSliceInput(new FileInputStream(file));
            sliceInput.skip(walMetadata.getBaseOffset());
        }
        catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
        return new WriteAheadLogReader(uri, sliceInput);
    }
}
