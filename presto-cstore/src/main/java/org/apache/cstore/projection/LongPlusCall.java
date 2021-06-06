package org.apache.cstore.projection;

import org.apache.cstore.column.VectorCursor;

public class LongPlusCall
        extends BinaryCall
{
    public LongPlusCall(int inputChannel0, int inputChannel1, int outputChannel)
    {
        super(inputChannel0, inputChannel1, outputChannel);
    }

    @Override
    protected void doProcess(VectorCursor op0, VectorCursor op1, VectorCursor out, int size)
    {
        for (int i = 0; i < size; i++) {
            out.writeLong(i, op0.readLong(i) + op1.readLong(i));
        }
    }

    @Override
    protected void doProcess(VectorCursor op0, VectorCursor op1, VectorCursor out, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            int position = positions[i];
            out.writeLong(i, op0.readLong(position) + op1.readLong(position));
        }
    }
}
