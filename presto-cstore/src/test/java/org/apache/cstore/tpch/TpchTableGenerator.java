package org.apache.cstore.tpch;

import io.airlift.compress.Compressor;
import io.airlift.tpch.TpchEntity;
import org.apache.cstore.coder.CoderFactory;
import org.apache.cstore.column.ChunkColumnWriter;
import org.apache.cstore.column.DoubleColumnPlainWriter;
import org.apache.cstore.column.IntColumnPlainWriter;
import org.apache.cstore.column.LongColumnPlainWriter;
import org.apache.cstore.column.StringEncodedColumnWriter;
import org.apache.cstore.dictionary.MutableTrieTree;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.meta.BitmapIndexMeta;
import org.apache.cstore.meta.ColumnMeta;
import org.apache.cstore.meta.TableMeta;
import org.apache.cstore.util.JsonUtil;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TpchTableGenerator<T extends TpchEntity>
{
    private final String dir;
    private final String table;
    private final String[] columnNames;
    private final String[] columnTypes;
    private final String metaFile;
    private final Iterable<T> records;
    private final Method[] getters;

    public TpchTableGenerator(Class<T> type, Iterable<T> records, String dir, String table, String metaFile, String fieldPrefix)
    {
        this.dir = dir;
        this.table = table;
        this.metaFile = metaFile;
        this.records = records;

        this.getters = Arrays.stream(type.getMethods())
                .filter(getter -> {
                    return getter.getParameterCount() == 0
                            && (getter.getModifiers() & (Modifier.STATIC | Modifier.NATIVE)) == 0
                            && !"getRowNumber".equals(getter.getName())
                            && getter.getName().startsWith("get");
                }).toArray(Method[]::new);

        this.columnNames = new String[getters.length];
        this.columnTypes = new String[getters.length];
        for (int j = 0; j < getters.length; j++) {
            Method getter = getters[j];
            columnNames[j] = fieldPrefix + getter.getName().substring(3).toLowerCase(Locale.getDefault());
            columnTypes[j] = getter.getReturnType().getSimpleName().toLowerCase(Locale.getDefault());
        }
    }

    public void run()
            throws Exception
    {
        File tableDirectory = new File(dir, table);
        if (tableDirectory.isDirectory() && tableDirectory.exists()) {
            tableDirectory.delete();
        }
        tableDirectory.mkdir();

        int columnCnt = columnTypes.length;

        final int pageSize = 64 << 10;
        String compressType = "lz4";
        final Compressor compressor = CoderFactory.INSTANCE.getCompressor(compressType);
        Map<String, CStoreColumnWriter<?>> writers = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            String type = columnTypes[i];
            String colName = columnNames[i];
            VectorWriterFactory binWriterFactory = new VectorWriterFactory(tableDirectory.getAbsolutePath(), colName, "bin");
            VectorWriterFactory zipWriterFactory = new VectorWriterFactory(tableDirectory.getAbsolutePath(), colName, "tar");
            switch (type) {
                case "int":
                    writers.put(colName, new ChunkColumnWriter<>(pageSize, compressor, zipWriterFactory, new IntColumnPlainWriter(binWriterFactory, false), false));
                    break;
                case "long":
                    writers.put(colName, new ChunkColumnWriter<>(pageSize, compressor, zipWriterFactory, new LongColumnPlainWriter(binWriterFactory, false), false));
                    break;
                case "double":
                    writers.put(colName, new ChunkColumnWriter<>(pageSize, compressor, zipWriterFactory, new DoubleColumnPlainWriter(binWriterFactory, false), false));
                    break;
                case "string":
                default:
                    StringEncodedColumnWriter stringEncodedVectorWriter = new StringEncodedColumnWriter(pageSize, compressor, new MutableTrieTree(), zipWriterFactory, false, false);
                    writers.put(colName, stringEncodedVectorWriter);
            }
        }

        int rowNum = 0;
        for (TpchEntity record : records) {
            for (int i = 0; i < columnCnt; i++) {
                String colName = columnNames[i];
                Object value = getters[i].invoke(record);
                CStoreColumnWriter vector = writers.get(colName);
                vector.write(value);
            }
            rowNum++;
        }

        for (CStoreColumnWriter<?> columnWriter : writers.values()) {
            columnWriter.close();
        }

        ColumnMeta[] columns = new ColumnMeta[columnCnt];
        List<BitmapIndexMeta> bitmapIndexes = new ArrayList<>();
        for (int i = 0; i < columnCnt; i++) {
            String type = columnTypes[i];
            ColumnMeta columnMeta = new ColumnMeta();
            columnMeta.setVersion("v1");
            columnMeta.setName(columnNames[i]);
            columnMeta.setTypeName(columnTypes[i]);
            columnMeta.setFileName(columnNames[i] + ".tar");
            columnMeta.setCompressType(compressType);
            columns[i] = columnMeta;

            if ("string".equals(type)) {
                BitmapIndexMeta indexMeta = new BitmapIndexMeta();
                indexMeta.setName(columnNames[i]);
                indexMeta.setFileName(columnNames[i] + ".bitmap");
                indexMeta.setCardinality(columnMeta.getCardinality());
                columnMeta.setDictionaryEncode(true);
                bitmapIndexes.add(indexMeta);
            }
        }

        TableMeta tableMeta = new TableMeta();
        tableMeta.setName(table);
        tableMeta.setColumns(columns);
        tableMeta.setRowCnt(rowNum);
        tableMeta.setBitmapIndexes(bitmapIndexes);
        tableMeta.setPageSize(pageSize);

        Files.write(Paths.get(tableDirectory.getAbsolutePath(), metaFile), JsonUtil.write(tableMeta));
    }
}
