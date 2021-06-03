package org.apache.cstore.tpch;

import io.airlift.tpch.Supplier;
import io.airlift.tpch.SupplierGenerator;
import org.testng.annotations.Test;

public class SupplierRunner
{
    @Test
    public void testSF1()
            throws Exception
    {
        TpchTableGenerator<Supplier> generator = new TpchTableGenerator<>(
                Supplier.class,
                new SupplierGenerator(1, 1, 1),
                "sample-data/tpch",
                "supplier", "meta.json", "s_");
        generator.run();
    }
}
