package org.apache.cstore.interpeter;

import org.apache.cstore.column.VectorCursor;
import org.apache.cstore.filter.SelectedPositions;

import java.util.List;

public class CStorePage
{
    private final List<VectorCursor> data;
    private final SelectedPositions selections;

    public CStorePage(List<VectorCursor> data, SelectedPositions selections)
    {
        this.data = data;
        this.selections = selections;
    }

    public List<VectorCursor> getData()
    {
        return data;
    }

    public SelectedPositions getSelections()
    {
        return selections;
    }
}
