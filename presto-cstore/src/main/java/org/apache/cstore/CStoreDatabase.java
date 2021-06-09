package org.apache.cstore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.CStoreConfig;
import com.google.inject.Inject;
import io.airlift.compress.Decompressor;
import org.apache.cstore.coder.CompressFactory;
import org.apache.cstore.column.BitmapColumnReader;
import org.apache.cstore.column.CStoreColumnLoader;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.meta.BitmapIndexMeta;
import org.apache.cstore.meta.ColumnMeta;
import org.apache.cstore.meta.DbMeta;
import org.apache.cstore.meta.TableMeta;
import org.apache.cstore.util.IOUtil;
import org.apache.cstore.util.JsonUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CStoreDatabase
{
    private static final Logger log = Logger.get(CStoreDatabase.class);

    private final String dataDirectory;
    private final Map<String, DbMeta> dbMetaMap;
    private final CompressFactory compressFactory;
    private final Map<String, Map<String, TableReader>> tableReaders;
    private final CStoreColumnLoader columnLoader;
    private boolean running;

    @Inject
    public CStoreDatabase(CStoreConfig config)
    {
        this.dataDirectory = config.getDataDirectory();
        this.dbMetaMap = new HashMap<>();
        this.compressFactory = new CompressFactory();
        this.tableReaders = new HashMap<>();
        this.columnLoader = new CStoreColumnLoader();
        this.running = false;

        setup();
    }

    public void setup()
    {
        if (running) {
            return;
        }
        running = true;
        log.info("setup database...");

        File dataFile = new File(dataDirectory);
        for (File dbFile : dataFile.listFiles()) {
            if (!dbFile.isDirectory() || dbFile.getName().startsWith(".")) {
                continue;
            }
            List<TableMeta> tableMetaList = new ArrayList<>();
            for (File tableFile : dbFile.listFiles()) {
                if (!tableFile.isDirectory() || tableFile.getName().startsWith(".")) {
                    continue;
                }
                File metaFile = new File(tableFile, "meta.json");
                if (!metaFile.exists()) {
                    continue;
                }
                TableMeta tableMeta = JsonUtil.read(metaFile, TableMeta.class);
                tableMetaList.add(tableMeta);
            }
            DbMeta dbMeta = new DbMeta();
            dbMeta.setName(dbFile.getName());
            dbMeta.setTables(tableMetaList);
            dbMetaMap.put(dbMeta.getName(), dbMeta);
        }

        for (DbMeta dbMeta : dbMetaMap.values()) {
            Map<String, TableReader> dbDataMap = tableReaders.computeIfAbsent(dbMeta.getName(),
                    (db) -> new HashMap<>());
            for (TableMeta tableMeta : dbMeta.getTables()) {
                TableReader tableDataMap = dbDataMap.computeIfAbsent(tableMeta.getName(), (k) -> new TableReader());
                File path = getTableFile(dbMeta.getName(), tableMeta.getName());
                for (ColumnMeta columnMeta : tableMeta.getColumns()) {
                    tableDataMap.columnReaderBuilders.computeIfAbsent(columnMeta.getName(), k -> {
                        Decompressor decompressor = compressFactory.getDecompressor(columnMeta.getCompressType());
                        Type type = mapType(columnMeta.getTypeName());
                        return columnLoader.openZipReader(tableMeta.getRowCnt(), tableMeta.getPageSize(), decompressor, path.getAbsolutePath(), columnMeta.getName(), type);
                    });
                }

                for (BitmapIndexMeta indexMeta : tableMeta.getBitmapIndexes()) {
                    tableDataMap.bitmapReaderBuilders.computeIfAbsent(indexMeta.getName(), column -> {
                        ByteBuffer buffer = IOUtil.mapFile(new File(path, indexMeta.getFileName()), FileChannel.MapMode.READ_ONLY);
                        return BitmapColumnReader.newBuilder(buffer);
                    });
                }
            }
        }
        log.info("database setup success");
    }

    public Map<String, DbMeta> getDbMetaMap()
    {
        return dbMetaMap;
    }

    public DbMeta getDbMeta(String name)
    {
        return dbMetaMap.get(name);
    }

    public TableMeta getTableMeta(String db, String table)
    {
        DbMeta dbMeta = dbMetaMap.get(db);
        return dbMeta.getTableMap().get(table);
    }

    public File getTableFile(String db, String table)
    {
        File dbFile = new File(dataDirectory, db);
        File tableFile = new File(dbFile, table);
        assert tableFile.exists() && tableFile.isDirectory();
        return tableFile;
    }

    public List<ColumnMeta> getColumn(String db, String table)
    {
        DbMeta dbMeta = dbMetaMap.get(db);
        TableMeta tableMeta = dbMeta.getTableMap().get(table);
        return tableMeta.getColumns();
    }

    public CStoreColumnReader getColumnReader(String db, String table, String column)
    {
        CStoreColumnReader reader = tableReaders.get(db).get(table).columnReaderBuilders.get(column).build();
        reader.setup();
        return reader;
    }

    public BitmapColumnReader getBitmapReader(String db, String table, String column)
    {
        BitmapColumnReader reader = tableReaders.get(db).get(table).bitmapReaderBuilders.get(column).build();
        reader.setup();
        return reader;
    }

    private Type mapType(String from)
    {
        switch (from) {
            case "int":
                return IntegerType.INTEGER;
            case "long":
                return BigintType.BIGINT;
            case "double":
                return DoubleType.DOUBLE;
            case "string":
                return VarcharType.VARCHAR;
            default:
        }
        throw new UnsupportedOperationException();
    }

    public void close()
    {
    }

    private static class TableReader
    {
        private final Map<String, CStoreColumnReader.Builder> columnReaderBuilders;
        private final Map<String, BitmapColumnReader.Builder> bitmapReaderBuilders;

        private TableReader()
        {
            this.columnReaderBuilders = new HashMap<>();
            this.bitmapReaderBuilders = new HashMap<>();
        }
    }

    public void createStagingTable(String db, String table)
    {
        File stagingDirectory = getTableStagingPath(db, table);
        if (stagingDirectory.exists()) {
            stagingDirectory.delete();
        }
        stagingDirectory.mkdir();
    }

    public void commitStagingTable(String db, String table)
            throws IOException
    {
        File dbDirectory = new File(dataDirectory, db);
        assert dbDirectory.isDirectory() && dbDirectory.exists();
        File stagingDirectory = getTableStagingPath(db, table);
        if (stagingDirectory.exists() && stagingDirectory.isDirectory()) {
            File tableDirectory = getTableFile(db, table);
            if (tableDirectory.exists()) {
                deleteFile(tableDirectory, true);
            }
            tableDirectory.mkdir();
            Files.move(Paths.get(stagingDirectory.getAbsolutePath()), Paths.get(tableDirectory.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static boolean deleteFile(File file, boolean deleteChildren)
    {
        if (file.isDirectory() && deleteChildren) {
            for (File child : file.listFiles()) {
                deleteFile(child, true);
            }
        }
        return file.delete();
    }

    public File getTableStagingPath(String db, String table)
    {
        File dbDirectory = new File(dataDirectory, db);
        assert dbDirectory.isDirectory() && dbDirectory.exists();
        return new File(dbDirectory, ".staging." + table);
    }
}
