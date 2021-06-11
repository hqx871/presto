//package github.cstore.interpeter;
//
//import com.facebook.airlift.log.Logger;
//import com.facebook.presto.common.type.TypeManager;
//import com.facebook.presto.cstore.CStoreColumnHandle;
//import com.facebook.presto.cstore.CStoreSplit;
//import com.facebook.presto.spi.ConnectorSession;
//import com.facebook.presto.spi.function.FunctionMetadataManager;
//import com.facebook.presto.spi.function.StandardFunctionResolution;
//import com.facebook.presto.spi.relation.RowExpression;
//import com.google.common.base.Stopwatch;
//import com.google.common.collect.Lists;
//import github.cstore.CStoreDatabase;
//import github.cstore.column.BitmapColumnReader;
//import github.cstore.column.CStoreColumnReader;
//import github.cstore.column.VectorCursor;
//import github.cstore.filter.IndexFilterInterpreter;
//import github.cstore.filter.SelectedPositions;
//
//import javax.annotation.Nullable;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//public class CStoreScanFilterOperator
//        implements CStoreOperator
//{
//    private static final Logger log = Logger.get(CStoreScanFilterOperator.class);
//
//    private final CStoreDatabase database;
//    private final TypeManager typeManager;
//    private final FunctionMetadataManager functionMetadataManager;
//    private final StandardFunctionResolution standardFunctionResolution;
//    private final ConnectorSession session;
//    private final CStoreSplit split;
//    private final CStoreColumnHandle[] columns;
//    @Nullable
//    private final RowExpression filter;
//    private Iterator<SelectedPositions> mask;
//    private final CStoreColumnReader[] columnReaders;
//    private long completedBytes;
//    private long completedPositions;
//    private long readTimeNanos;
//    private long systemMemoryUsage;
//    private final int vectorSize;
//    private final VectorCursor[] cursors;
//
//    public CStoreScanFilterOperator(CStoreDatabase database,
//            TypeManager typeManager,
//            FunctionMetadataManager functionMetadataManager,
//            StandardFunctionResolution standardFunctionResolution,
//            ConnectorSession session,
//            CStoreSplit split,
//            CStoreColumnHandle[] columnHandles,
//            @Nullable RowExpression filter)
//    {
//        this.database = database;
//        this.typeManager = typeManager;
//        this.functionMetadataManager = functionMetadataManager;
//        this.standardFunctionResolution = standardFunctionResolution;
//        this.session = session;
//        this.vectorSize = 1024;
//        this.columns = columnHandles;
//        this.split = split;
//        this.columnReaders = new CStoreColumnReader[columnHandles.length];
//        this.filter = filter;
//        this.cursors = new VectorCursor[columns.length];
//
//        setup();
//    }
//
//    public void setup()
//    {
//        Map<String, CStoreColumnReader> columnReaderMap = new HashMap<>();
//        for (int i = 0; i < columns.length; i++) {
//            columnReaders[i] = database.getColumnReader(split.getSchema(), split.getTable(), columns[i].getColumnName());
//            columnReaderMap.put(columns[i].getColumnName(), columnReaders[i]);
//        }
//        IndexFilterInterpreter indexFilterInterpreter = new IndexFilterInterpreter(this.typeManager,
//                this.functionMetadataManager,
//                this.standardFunctionResolution,
//                this.session,
//                this.database);
//        IndexFilterInterpreter.Context interpreterContext = new IndexFilterInterpreter.Context()
//        {
//            @Override
//            public CStoreColumnReader getColumnReader(String column)
//            {
//                return columnReaderMap.get(column);
//            }
//
//            @Override
//            public BitmapColumnReader getBitmapReader(String column)
//            {
//                return database.getBitmapReader(split.getSchema(), split.getTable(), column);
//            }
//        };
//        this.mask = indexFilterInterpreter.compute(filter, split.getRowCount(), vectorSize, interpreterContext);
//        for (int i = 0; i < columns.length; i++) {
//            cursors[i] = columnReaders[i].createVectorCursor(vectorSize);
//            this.systemMemoryUsage += cursors[i].getSizeInBytes();
//        }
//    }
//
//    //@Override
//    public long getCompletedBytes()
//    {
//        return completedBytes;
//    }
//
//    //@Override
//    public long getCompletedPositions()
//    {
//        return completedPositions;
//    }
//
//    //@Override
//    public long getReadTimeNanos()
//    {
//        return readTimeNanos;
//    }
//
//    //@Override
//    public boolean isFinished()
//    {
//        return !mask.hasNext();
//    }
//
//    @Override
//    public CStorePage getNextPage()
//    {
//        if (isFinished()) {
//            this.completedPositions = split.getRowCount();
//            return null;
//        }
//        Stopwatch stopwatch = Stopwatch.createStarted();
//
//        SelectedPositions selection = mask.next();
//        for (int i = 0; i < cursors.length; i++) {
//            VectorCursor cursor = cursors[i];
//            CStoreColumnReader columnReader = columnReaders[i];
//            if (selection.isList()) {
//                columnReader.read(selection.getPositions(), selection.getOffset(), selection.size(), cursor);
//            }
//            else {
//                columnReader.read(selection.getOffset(), selection.size(), cursor);
//            }
//        }
//        for (int i = 0; i < cursors.length; i++) {
//            VectorCursor cursor = cursors[i];
//            this.completedBytes += cursor.getSizeInBytes();
//        }
//
//        if (selection.size() > 0) {
//            if (selection.isList()) {
//                this.completedPositions = selection.getPositions()[selection.size() - 1];
//            }
//            else {
//                this.completedPositions = selection.getOffset() + selection.size();
//            }
//        }
//
//        this.readTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
//        return new CStorePage(Lists.newArrayList(cursors), selection);
//    }
//
//    //@Override
//    public long getSystemMemoryUsage()
//    {
//        return systemMemoryUsage;
//    }
//
//    @Override
//    public void close()
//            throws IOException
//    {
//        for (CStoreColumnReader columnReader : columnReaders) {
//            columnReader.close();
//        }
//        log.info("finished, read time mills:%d", TimeUnit.NANOSECONDS.toMillis(readTimeNanos));
//    }
//}
