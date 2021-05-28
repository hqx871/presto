package org.apache.cstore.columnar;

import com.facebook.presto.common.block.BlockBuilder;

public interface ColumnData
{
    int read(SelectedPositions selection, BlockBuilder blockBuilder);
}
