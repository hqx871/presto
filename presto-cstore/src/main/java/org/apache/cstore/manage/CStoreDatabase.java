package org.apache.cstore.manage;

import com.facebook.presto.cstore.CStoreConfig;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.cstore.meta.ColumnMeta;
import org.apache.cstore.meta.DbMeta;
import org.apache.cstore.meta.TableMeta;
import org.apache.cstore.util.JsonUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CStoreDatabase
{
    private final String dataDirectory;
    private final Map<String, DbMeta> dbMetaMap;

    @Inject
    public CStoreDatabase(CStoreConfig config)
    {
        this.dataDirectory = config.getDataDirectory();
        this.dbMetaMap = new HashMap<>();

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
            DbMeta dbMeta = new DbMeta(dbFile.getName(), tableMetaList);
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
        return Lists.newArrayList(tableMeta.getColumn());
    }

    public void close()
    {
    }
}
