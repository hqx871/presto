package github.cstore.interpeter;

import github.cstore.aggregation.AggregationCursor;
import github.cstore.aggregation.AggregationCursorFactory;
import github.cstore.aggregation.PartialAggregator;
import github.cstore.column.VectorCursor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class CStoreAggregateOperator
        implements CStoreOperator
{
    private final CStoreOperator source;
    private final PartialAggregator partialAggregator;
    private boolean process;
    private final AggregationCursorFactory aggregationCursorFactory;
    private Iterator<CStorePage> pageIterator;

    public CStoreAggregateOperator(CStoreOperator source, PartialAggregator partialAggregator)
    {
        this.source = source;
        this.partialAggregator = partialAggregator;
        this.aggregationCursorFactory = new AggregationCursorFactory();
        this.process = true;
        this.pageIterator = Collections.emptyIterator();
    }

    @Override
    public void setup()
    {
        source.setup();
    }

    @Override
    public CStorePage getNextPage()
    {
        if (process) {
            CStorePage page = source.getNextPage();
            while (page != null) {
                process(page);
            }
            pageIterator = toPage();
        }
        process = false;
        return pageIterator.hasNext() ? pageIterator.next() : null;
    }

    private void process(CStorePage page)
    {
        List<AggregationCursor> cursorWrappers = new ArrayList<>();
        for (VectorCursor cursor : page.getData()) {
            cursorWrappers.add(aggregationCursorFactory.wrap(cursor));
        }
        partialAggregator.addPage(cursorWrappers, page.getSelections());
    }

    private Iterator<CStorePage> toPage()
    {
        Iterator<ByteBuffer> rawIterator = partialAggregator.getResult();
        return new Iterator<CStorePage>()
        {
            @Override
            public boolean hasNext()
            {
                return rawIterator.hasNext();
            }

            @Override
            public CStorePage next()
            {
                return null;
            }
        };
    }

    @Override
    public void close()
            throws IOException
    {
        source.close();
    }
}
