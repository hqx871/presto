package org.apache.cstore.column;

import io.airlift.compress.Compressor;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.RoaringBitmapAdapter;
import org.apache.cstore.dictionary.MutableTrieTree;
import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class StringEncodedColumnWriter
        extends AbstractColumnWriter<String>
{
    private StreamWriter idWriter;
    private StreamWriter bitmapWriter;

    private final MutableTrieTree dict;
    private final SortedMap<Integer, MutableRoaringBitmap> bitmaps;
    private int rowNum;
    private File idFile;
    private final boolean writeTreeDictionary;
    private final int pageSize;
    private final Compressor compressor;

    public StringEncodedColumnWriter(int pageSize, Compressor compressor, MutableTrieTree dict, VectorWriterFactory writerFactory, boolean writeTreeDictionary, boolean delete)
    {
        super(writerFactory, delete);
        this.idFile = writerFactory.newFile("id");
        this.writeTreeDictionary = writeTreeDictionary;
        this.pageSize = pageSize;
        this.compressor = compressor;
        File bitmapFile = writerFactory.newFile("bitmap");
        this.idWriter = new OutputStreamWriter(IOUtil.openFileStream(idFile));

        this.bitmapWriter = new OutputStreamWriter(IOUtil.openFileStream(bitmapFile));
        this.dict = dict;

        this.bitmaps = new TreeMap<>();
        this.bitmaps.put(0, new MutableRoaringBitmap());
        this.rowNum = 0;
    }

    @Override
    public int write(String value)
    {
        int id = dict.encode(value);
        idWriter.putInt(id);
        MutableRoaringBitmap bitmap = bitmaps.computeIfAbsent(id, k -> new MutableRoaringBitmap());
        bitmap.add(rowNum++);
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (idWriter != null) {
            idWriter.close();
        }
        idWriter = null;
        if (bitmapWriter != null) {
            bitmapWriter.close();
        }
        bitmapWriter = null;
        if (idFile != null) {
            idFile.delete();
        }
        idFile = null;
        super.close();
    }

    @Override
    public void doFlush()
            throws IOException
    {
        idWriter.flush();

        int[] newIds = dict.sortValue();
        int sstSize = dict.writeSst(streamWriter, writerFactory);
        streamWriter.putInt(sstSize);
        int dataSize = writeData(pageSize, compressor, streamWriter, dict.maxEncodeId(), newIds);
        streamWriter.putInt(dataSize);
        streamWriter.flush();

        if (writeTreeDictionary) {
            File treeFile = writerFactory.newFile("dict");
            dict.writeTrieTree(new OutputStreamWriter(writerFactory.createFileStream(treeFile)));
        }

        writeBitmap(bitmapWriter, newIds);
        this.bitmapWriter.flush();
    }

    private int writeBitmap(StreamWriter mergeStream, int[] newIds)
            throws IOException
    {
        BinaryOffsetColumnWriter<Bitmap> bitmapWriter = new BinaryOffsetColumnWriter<>(
                new VectorWriterFactory(writerFactory.getDir(), writerFactory.getName(), "bitmap"),
                BitmapColumnReader.coder, false);

        SortedMap<Integer, MutableRoaringBitmap> newBitmaps = new TreeMap<>();
        bitmaps.forEach((oldId, bitmap) -> {
            int newId = newIds[oldId];
            newBitmaps.put(newId, bitmap);
        });
        for (Entry<Integer, MutableRoaringBitmap> valueBitmap : newBitmaps.entrySet()) {
            MutableRoaringBitmap bitmap = valueBitmap.getValue();
            bitmapWriter.write(new RoaringBitmapAdapter(bitmap));
        }
        bitmapWriter.flush();
        int size = bitmapWriter.appendTo(mergeStream);
        bitmapWriter.close();
        return size;
    }

    private int writeData(int pageSize, Compressor compressor, StreamWriter output, int ceilId, int[] newIds)
            throws IOException
    {
        IntBuffer ids = IOUtil.mapFile(idFile, MapMode.READ_ONLY).asIntBuffer();

        int size = 0;
        VectorWriterFactory idWriterFactory = new VectorWriterFactory(writerFactory.getDir(), writerFactory.getName() + ".id", "bin");
        VectorWriterFactory tarWriterFactory = new VectorWriterFactory(writerFactory.getDir(), writerFactory.getName() + ".id", "tar");

        if (ceilId <= Byte.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_BYTE);
            ByteColumnPlainWriter bytePlainWriter = new ByteColumnPlainWriter(idWriterFactory, true);
            ChunkColumnWriter<Byte> pageWriter = new ChunkColumnWriter<>(pageSize, compressor, tarWriterFactory, bytePlainWriter, true);

            while (ids.hasRemaining()) {
                pageWriter.write((byte) newIds[ids.get()]);
            }
            pageWriter.flush();
            size = Byte.BYTES + pageWriter.appendTo(output);
            pageWriter.close();
        }
        else if (ceilId <= Short.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_SHORT);
            ShortColumnPlainWriter shortVectorWriter = new ShortColumnPlainWriter(idWriterFactory, true);
            ChunkColumnWriter<Short> pageWriter = new ChunkColumnWriter<>(pageSize, compressor, tarWriterFactory, shortVectorWriter, true);
            while (ids.hasRemaining()) {
                pageWriter.write((short) newIds[ids.get()]);
            }
            pageWriter.flush();
            size = Byte.BYTES + pageWriter.appendTo(output);
            pageWriter.close();
        }
        else {
            output.putByte(ColumnEncodingId.PLAIN_INT);
            IntColumnPlainWriter intVectorWriter = new IntColumnPlainWriter(idWriterFactory, true);
            ChunkColumnWriter<Integer> pageWriter = new ChunkColumnWriter<>(pageSize, compressor, tarWriterFactory, intVectorWriter, true);
            while (ids.hasRemaining()) {
                pageWriter.write(newIds[ids.get()]);
            }
            pageWriter.flush();
            size = Byte.BYTES + pageWriter.appendTo(output);
            pageWriter.close();
        }
        return size;
    }
}
