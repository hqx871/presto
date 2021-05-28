package org.apache.cstore.columnar;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.IntBuffer;

public class IntColumnar
        implements ColumnData
{
    private final IntBuffer buffer;
    private final int count;

    public IntColumnar(IntBuffer buffer, int count)
    {
        this.buffer = buffer;
        this.count = count;
    }

    @Override
    public int read(SelectedPositions selection, BlockBuilder blockBuilder)
    {
        if (selection.isList()) {
            int[] positions = selection.getPositions();
            for (int i = selection.getOffset(); i < selection.size(); i++) {
                blockBuilder.writeInt(buffer.get(positions[i]));
            }
        }
        else {
            for (int i = 0; i < selection.size(); i++) {
                blockBuilder.writeInt(buffer.get(i + selection.getOffset()));
            }
        }
        return selection.size();
    }
}
