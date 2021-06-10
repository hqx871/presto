package github.cstore.projection;

import github.cstore.column.VectorCursor;

public class DoublePlusCall
        extends BinaryCall
{
    public DoublePlusCall(int inputChannel0, int inputChannel1, int outputChannel)
    {
        super(inputChannel0, inputChannel1, outputChannel);
    }

    @Override
    protected void doProcess(VectorCursor op0, VectorCursor op1, VectorCursor out, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            int position = positions[i];
            out.writeDouble(i, op0.readDouble(position) + op1.readDouble(position));
        }
    }

    @Override
    protected void doProcess(VectorCursor op0, VectorCursor op1, VectorCursor out, int size)
    {
        for (int i = 0; i < size; i++) {
            out.writeDouble(i, op0.readDouble(i) + op1.readDouble(i));
        }
    }
}
