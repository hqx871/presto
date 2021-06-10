package github.cstore.interpeter;

import github.cstore.projection.ScalarCall;

import java.io.IOException;
import java.util.List;

public class CStoreProjectOperator
        implements CStoreOperator
{
    private final CStoreOperator source;
    private final List<ScalarCall> projections;

    public CStoreProjectOperator(CStoreOperator source, List<ScalarCall> projections)
    {
        this.source = source;
        this.projections = projections;
    }

    @Override
    public void setup()
    {
        source.setup();
    }

    @Override
    public CStorePage getNextPage()
    {
        CStorePage page = source.getNextPage();
        if (page != null) {
            project(page);
        }
        return page;
    }

    private void project(CStorePage page)
    {
        assert page.getSelections().getOffset() == 0;
        if (page.getSelections().isList()) {
            for (ScalarCall projection : projections) {
                projection.process(page.getData(), page.getSelections().getPositions(), page.getSelections().size());
            }
        }
        else {
            for (ScalarCall projection : projections) {
                projection.process(page.getData(), page.getSelections().size());
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        source.close();
    }
}
