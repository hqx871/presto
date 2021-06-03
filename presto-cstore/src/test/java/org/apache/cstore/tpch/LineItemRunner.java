package org.apache.cstore.tpch;

import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import org.testng.annotations.Test;

public class LineItemRunner
{
    @Test
    public void testSF1()
            throws Exception
    {
        TpchTableGenerator<LineItem> generator = new TpchTableGenerator<>(
                LineItem.class,
                new LineItemGenerator(1, 1, 1),
                "sample-data/tpch",
                "lineitem", "meta.json", "l_");
        generator.run();
    }
}
