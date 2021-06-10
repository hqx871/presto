package github.cstore.interpeter;

import java.io.IOException;

public interface CStoreOperator
{
    void setup();

    CStorePage getNextPage();

    void close()
            throws IOException;
}
