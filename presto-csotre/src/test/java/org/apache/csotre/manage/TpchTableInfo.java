package org.apache.csotre.manage;

import java.io.File;

public class TpchTableInfo
{
    private static final String dbDir = "/Users/huangqixiang/tmp/cstore/tpch";
    private static final String[] ddl_tpch_lineitem = new String[] {
            "l_orderkey", "long",
            "l_partkey", "long",
            "l_suppkey", "long",
            "l_linenumber", "long",
            "l_quantity", "double",
            "l_extendedprice", "double",
            "l_discount", "double",
            "l_tax", "double",
            "l_returnflag", "string",
            "l_linestatus", "string",
            "l_shipdate", "string",
            "l_commitdate", "string",
            "l_receiptdate", "string",
            "l_shipinstruct", "string",
            "l_shipmode", "string",
            "l_comment", "string"
    };
    private static final String[] ddl_supplier = new String[] {
            "s_suppkey", "long",
            "s_name", "string",
            "s_address", "string",
            "s_nationkey", "long",
            "s_phone", "string",
            "s_acctbal", "double",
            "s_comment", "string",
    };

    private final String table;
    private final int columnCnt;
    private final String[] columns;
    private final String[] types;
    private final File dataDir;

    public static final TpchTableInfo TPCH_LINEITEM_SMALL = new TpchTableInfo("tpch_lineitem_small_v2",
            ddl_tpch_lineitem,
            2);

    public static final TpchTableInfo TPCH_LINEITEM = new TpchTableInfo("tpch_lineitem_small",
            ddl_tpch_lineitem,
            2);

    public static final TpchTableInfo TPCH_SUPPLIER = new TpchTableInfo("supplier",
            ddl_supplier,
            2);

    private TpchTableInfo(String table, String[] ddl, int step)
    {
        this.table = table;
        this.columnCnt = ddl.length / step;

        columns = new String[columnCnt];
        types = new String[columnCnt];
        for (int i = 0; i < columnCnt; i++) {
            columns[i] = ddl[i * 2];
            types[i] = ddl[i * 2 + 1];
        }
        this.dataDir = new File(dbDir, table);
    }

    public String getTable()
    {
        return table;
    }

    public int getColumnCnt()
    {
        return columnCnt;
    }

    public String[] getColumns()
    {
        return columns;
    }

    public String[] getTypes()
    {
        return types;
    }

    public String getDbDir()
    {
        return dbDir;
    }

    public File getDataDir()
    {
        return dataDir;
    }
}
