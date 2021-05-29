package org.apache.cstore.manage;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.cstore.column.DoubleColumnWriter;
import org.apache.cstore.column.LongColumnWriter;
import org.apache.cstore.column.StringEncodedColumnWriter;
import org.apache.cstore.dictionary.TrieHeapTree;
import org.apache.cstore.io.ColumnWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.meta.ColumnMeta;
import org.apache.cstore.meta.TableMeta;
import org.apache.cstore.util.JsonUtil;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class CsvTool
{
    private final String csv;
    private final char separator;
    private final String dir;
    private final String table;
    private final String[] columnNames;
    private final String[] columnTypes;
    private final String metaFile;

    public CsvTool(String csv, char separator, String dir, String table, String[] columnNames, String[] columnTypes, String metaFile)
    {
        this.csv = csv;
        this.separator = separator;
        this.dir = dir;
        this.table = table;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.metaFile = metaFile;
    }

    public void run()
            throws IOException
    {
        String dataDir = dir + "/" + table;
        File dataFile = new File(dataDir);
        if (dataFile.isDirectory() && dataFile.exists()) {
            dataFile.delete();
        }
        dataFile.mkdir();

        int columnCnt = columnTypes.length;

        CSVParser records = CSVFormat.DEFAULT.withHeader(columnNames)
                .withDelimiter(separator)
                .parse(new FileReader(csv));

        Map<String, ColumnWriter<?>> writers = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            String type = columnTypes[i];
            String colName = columnNames[i];
            VectorWriterFactory writerFactor = new VectorWriterFactory(dataDir, colName);
            switch (type) {
                case "long":
                    writers.put(colName, new LongColumnWriter(writerFactor));
                    break;
                case "double":
                    writers.put(colName, new DoubleColumnWriter(writerFactor));
                    break;
                case "string":
                default:
                    StringEncodedColumnWriter stringEncodedVectorWriter = new StringEncodedColumnWriter(new TrieHeapTree(), writerFactor);
                    writers.put(colName, stringEncodedVectorWriter);
            }
        }

        int rowNum = 0;
        for (CSVRecord record : records) {
            for (int i = 0; i < columnCnt; i++) {
                String colName = columnNames[i];
                String value = record.get(colName);
                String type = columnTypes[i];

                ColumnWriter vector = writers.get(colName);

                switch (type) {
                    case "long":
                        vector.write(Long.parseLong(value));
                        break;
                    case "double":
                        vector.write(Double.parseDouble(value));
                        break;
                    case "string":
                    default:
                        vector.write(value);
                }
            }
            rowNum++;
        }

        for (ColumnWriter columnWriter : writers.values()) {
            columnWriter.close();
        }

        ColumnMeta[] columns = new ColumnMeta[columnCnt];
        for (int i = 0; i < columnCnt; i++) {
            String type = columnTypes[i];
            if ("string".equals(type)) {
                ColumnMeta columnMeta = new ColumnMeta();
                columnMeta.setVersion("v2");
                columnMeta.setName(columnNames[i]);
                columnMeta.setTypeName(columnTypes[i]);
                columnMeta.setFileName(columnNames[i] + ".bin");

                columns[i] = columnMeta;
            }
            else {
                ColumnMeta columnMeta = new ColumnMeta();
                columnMeta.setVersion("v1");
                columnMeta.setName(columnNames[i]);
                columnMeta.setTypeName(columnTypes[i]);
                columnMeta.setFileName(columnNames[i] + ".bin");
                columns[i] = columnMeta;
            }
        }

        TableMeta tableMeta = new TableMeta();
        tableMeta.setName(table);
        tableMeta.setColumn(columns);
        tableMeta.setRowCnt(rowNum);

        Files.write(Paths.get(dataDir, metaFile), JsonUtil.write(tableMeta));
    }
}
