package github.cstore.projection;

import github.cstore.column.VectorCursor;

import java.util.List;

public interface ScalarCall
{
    int getOutputChannel();

    void process(List<? extends VectorCursor> page, int size);

    void process(List<? extends VectorCursor> page, int[] positions, int size);
}
