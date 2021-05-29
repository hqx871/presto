package org.apache.cstore.manage;

import org.testng.annotations.Test;

import java.io.IOException;

public class CSVImporterTest
{
    @Test
    public void testImportLineItemSmall()
            throws IOException
    {
        TpchTableInfo lineItem = TpchTableInfo.TPCH_LINEITEM_SMALL;
        String csv = "/Users/huangqixiang/github/tpch-2.18.0_rc2/tool/lineitem_100.tbl";

        CsvTool writer = new CsvTool(csv, '|', lineItem.getDbDir(), lineItem.getTable(), lineItem.getColumns(), lineItem.getTypes(), "meta.json");
        writer.run();
    }

    @Test
    public void testImportLineItem()
            throws IOException
    {
        TpchTableInfo lineItem = TpchTableInfo.TPCH_LINEITEM;
        String csv = "/Users/huangqixiang/github/tpch-2.18.0_rc2/tool/lineitem.tbl";

        CsvTool writer = new CsvTool(csv, '|', lineItem.getDbDir(), lineItem.getTable(), lineItem.getColumns(), lineItem.getTypes(), "meta.json");
        writer.run();
    }

    @Test
    public void testImportSupplier()
            throws IOException
    {
        TpchTableInfo tableInfo = TpchTableInfo.TPCH_SUPPLIER;
        String csv = "/Users/huangqixiang/github/tpch-2.18.0_rc2/tool/supplier.tbl";
        CsvTool writer = new CsvTool(csv, '|', tableInfo.getDbDir(), tableInfo.getTable(), tableInfo.getColumns(), tableInfo.getTypes(), "meta.json");
        writer.run();
    }
}
