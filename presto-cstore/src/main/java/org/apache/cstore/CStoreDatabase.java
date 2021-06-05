package org.apache.cstore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.CStoreConfig;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.compress.Decompressor;
import org.apache.cstore.coder.CoderFactory;
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CStoreDatabase
{
    private static final Logger log = Logger.get(CStoreDatabase.class);

    private final String dataDirectory;
    private final Map<String, DbMeta> dbMetaMap;
    private final CoderFactory coderFactory;
    private final Map<String, Map<String, Factories>> columnFactories;
    private final CStoreColumnLoader columnLoader;
    private boolean running;

    @Inject
    public CStoreDatabase(CStoreConfig config)
    {
        this.dataDirectory = config.getDataDirectory();
        this.dbMetaMap = new HashMap<>();
        this.coderFactory = new CoderFactory();
        this.columnFactories = new HashMap<>();
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
            if (!dbFile.isDirectory()) {
                continue;
            }
            List<TableMeta> tableMetaList = new ArrayList<>();
            for (File tableFile : dbFile.listFiles()) {
                if (!tableFile.isDirectory()) {
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
            Map<String, Factories> dbDataMap = columnFactories.computeIfAbsent(dbMeta.getName(),
                    (db) -> new HashMap<>());
            for (TableMeta tableMeta : dbMeta.getTables()) {
                Factories tableDataMap = dbDataMap.computeIfAbsent(tableMeta.getName(), (k) -> new Factories());
                String path = getTablePath(dbMeta.getName(), tableMeta.getName());
                for (ColumnMeta columnMeta : tableMeta.getColumns()) {
                    tableDataMap.columnReaderFactories.computeIfAbsent(columnMeta.getName(), k -> {
                        Decompressor decompressor = coderFactory.getDecompressor(columnMeta.getCompressType());
                        Type type = mapType(columnMeta.getTypeName());
                        return columnLoader.open(tableMeta.getRowCnt(), tableMeta.getPageSize(), decompressor, path, columnMeta.getName(), type);
                    });
                }

                for (BitmapIndexMeta indexMeta : tableMeta.getBitmapIndexes()) {
                    tableDataMap.bitmapReaderFactories.computeIfAbsent(indexMeta.getName(), column -> {
                        ByteBuffer buffer = IOUtil.mapFile(new File(getTablePath(dbMeta.getName(), tableMeta.getName()),
                                indexMeta.getFileName()), FileChannel.MapMode.READ_ONLY);
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

    public String getTablePath(String db, String table)
    {
        File dbFile = new File(dataDirectory, db);
        File tableFile = new File(dbFile, table);
        return tableFile.getAbsolutePath();
    }

    public List<ColumnMeta> getColumn(String db, String table)
    {
        DbMeta dbMeta = dbMetaMap.get(db);
        TableMeta tableMeta = dbMeta.getTableMap().get(table);
        return Lists.newArrayList(tableMeta.getColumns());
    }

    public CStoreColumnReader getColumnReader(String db, String table, String column)
    {
        CStoreColumnReader reader = columnFactories.get(db).get(table).columnReaderFactories.get(column).duplicate();
        reader.setup();
        return reader;
    }

    public BitmapColumnReader getBitmapReader(String db, String table, String column)
    {
        BitmapColumnReader reader = columnFactories.get(db).get(table).bitmapReaderFactories.get(column).duplicate();
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

    private static class Factories
    {
        private final Map<String, CStoreColumnReader.Builder> columnReaderFactories;
        private final Map<String, BitmapColumnReader.Builder> bitmapReaderFactories;

        private Factories()
        {
            this.columnReaderFactories = new HashMap<>();
            this.bitmapReaderFactories = new HashMap<>();
        }
    }
}
