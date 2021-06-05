package org.apache.cstore;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.cstore.CStoreConfig;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.compress.Decompressor;
import org.apache.cstore.coder.CoderFactory;
import org.apache.cstore.column.BitmapColumnReader;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.CStoreColumnReaderFactory;
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
    private final String dataDirectory;
    private final Map<String, DbMeta> dbMetaMap;
    private final CoderFactory coderFactory;

    @Inject
    public CStoreDatabase(CStoreConfig config)
    {
        this.dataDirectory = config.getDataDirectory();
        this.dbMetaMap = new HashMap<>();
        this.coderFactory = CoderFactory.INSTANCE;

        open();
    }

    public void open()
    {
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

    public CStoreColumnReader getColumnReader(String db, String table, ColumnMeta column, Type type)
    {
        return getColumnReader(db, getTableMeta(db, table), column, type);
    }

    public CStoreColumnReader getColumnReader(String db, TableMeta tableMeta, ColumnMeta column, Type type)
    {
        Decompressor decompressor = coderFactory.getDecompressor(column.getCompressType());
        String path = getTablePath(db, tableMeta.getName());
        return new CStoreColumnReaderFactory().open(tableMeta.getRowCnt(), tableMeta.getPageSize(), decompressor, path, column.getName(), type);
    }

    public BitmapColumnReader getBitmapReader(String db, String table, String column)
    {
        TableMeta tableMeta = getTableMeta(db, table);
        BitmapIndexMeta indexMeta = tableMeta.getBitmap(column);
        ByteBuffer buffer = IOUtil.mapFile(new File(getTablePath(db, table), indexMeta.getFileName()), FileChannel.MapMode.READ_ONLY);
        return BitmapColumnReader.decode(buffer);
    }

    public void close()
    {
    }
}
