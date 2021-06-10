package github.cstore.interpeter;

import com.facebook.presto.spi.relation.RowExpression;
import github.cstore.column.VectorCursor;
import github.cstore.projection.ScalarCall;
import github.cstore.projection.ScalarExpressionInterpreter;

import java.io.IOException;

public class CStoreFilterOperator
        implements CStoreOperator
{
    private final CStoreOperator source;
    private final RowExpression filter;
    private ScalarCall call;

    public CStoreFilterOperator(CStoreOperator source, RowExpression filter)
    {
        this.source = source;
        this.filter = filter;
    }

    @Override
    public void setup()
    {
        source.setup();
        call = filter.accept(new ScalarExpressionInterpreter(), new ScalarExpressionInterpreter.Context());
    }

    @Override
    public CStorePage getNextPage()
    {
        CStorePage page = getNextPage();
        if (page == null) {
            return null;
        }
        assert page.getSelections().getOffset() == 0 && !page.getSelections().isList();
        if (page.getSelections().isList()) {
            call.process(page.getData(), page.getSelections().getPositions(), page.getSelections().size());
        }
        else {
            call.process(page.getData(), page.getSelections().size());
        }
        VectorCursor selections = page.getData().get(call.getOutputChannel());
        return null;
    }

    @Override
    public void close()
            throws IOException
    {
        source.close();
    }
}
