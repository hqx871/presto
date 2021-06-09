package org.apache.cstore.dictionary;

import com.google.common.base.Preconditions;
import org.apache.cstore.sort.BufferComparator;
import org.apache.cstore.coder.BufferCoder;
import org.apache.cstore.column.BinaryOffsetColumnWriter;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.BufferUtil;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;

public class MutableTrieTree
        extends MutableStringDictionary
{
    private int nextId;
    private final TrieTreeNode root;
    private byte nullId;
    private final List<String> noNullValues;

    public MutableTrieTree()
    {
        this.root = new TrieTreeNode(INVALID_ID, new char[0], new TrieTreeNode[0]);
        this.nullId = getNullValueId();
        this.noNullValues = new ArrayList<>();
        this.nextId = getNonNullValueStartId();
    }

    private int addNoNull(@Nonnull char[] value)
    {
        if (value.length == 0) {
            if (root.getId() == INVALID_ID) {
                root.setId(nextId(""));
            }
            return root.getId();
        }
        return addNoEmpty(root, value, 0);
    }

    private int nextId(String value)
    {
        noNullValues.add(value);
        return nextId++;
    }

    @Override
    public int encode(String value)
    {
        if (value == null) {
            nullId = getNullValueId();
            return nullId;
        }
        return addNoNull(value.toCharArray());
    }

    private int addNoEmpty(TrieTreeNode curNode, char[] value, int start)
    {
        int pos = binarySearch(curNode.getChildren(), 0, curNode.getChildren().length - 1, value, start);
        if (pos >= 0) {
            return splitNode(curNode, pos, value, start);
        }
        pos = -pos - 1;

        TrieTreeNode node = new TrieTreeNode(nextId(new String(value)), Arrays.copyOfRange(value, start, value.length), new TrieTreeNode[0]);
        TrieTreeNode[] children = new TrieTreeNode[curNode.getChildren().length + 1];
        if (pos > 0) {
            System.arraycopy(curNode.getChildren(), 0, children, 0, pos);
        }
        children[pos] = node;
        if (pos < curNode.getChildren().length) {
            System.arraycopy(curNode.getChildren(), pos, children, pos + 1, curNode.getChildren().length - pos);
        }
        curNode.setChildren(children);
        return node.getId();
    }

    private int splitNode(TrieTreeNode parent, int pos, char[] value, int start)
    {
        TrieTreeNode sameNode = parent.getChildren()[pos];
        int sameLength = sameLength(sameNode.getValue(), value, start);
        if (sameLength + start == value.length) {
            if (sameLength == sameNode.getValue().length) {
                if (sameNode.getId() == INVALID_ID) {
                    sameNode.setId(nextId(new String(value)));
                }
                return sameNode.getId();
            }
            else {
                TrieTreeNode node = new TrieTreeNode(nextId(new String(value)), Arrays.copyOfRange(value, start, value.length), new TrieTreeNode[] {sameNode});
                parent.getChildren()[pos] = node;
                sameNode.setValue(Arrays.copyOfRange(sameNode.getValue(), sameLength, sameNode.getValue().length));
                return node.getId();
            }
        }
        else if (sameLength == sameNode.getValue().length) {
            return addNoEmpty(sameNode, value, start + sameLength);
        }
        else {
            sameNode.setValue(Arrays.copyOfRange(sameNode.getValue(), sameLength, sameNode.getValue().length));
            TrieTreeNode newNode = new TrieTreeNode(nextId(new String(value)), Arrays.copyOfRange(value, start + sameLength, value.length), new TrieTreeNode[0]);

            TrieTreeNode[] children = new TrieTreeNode[] {sameNode, newNode};
            Arrays.sort(children);
            TrieTreeNode node = new TrieTreeNode(INVALID_ID, Arrays.copyOfRange(value, start, start + sameLength), children);
            parent.getChildren()[pos] = node;
            return newNode.getId();
        }
    }

    private int binarySearch(TrieTreeNode[] nodes, int from, int to, char[] value, int start)
    {
        if (from > to) {
            return -1 - from;
        }
        int middle = (from + to) / 2;

        int compared = value[start] - nodes[middle].getValue()[0];
        if (compared == 0) {
            return middle;
        }
        if (compared > 0) {
            return binarySearch(nodes, middle + 1, to, value, start);
        }
        else {
            return binarySearch(nodes, from, middle - 1, value, start);
        }
    }

    private int sameLength(char[] the, char[] that, int start)
    {
        int n = Math.min(the.length, that.length - start);
        for (int i = 0; i < n; i++) {
            if (the[i] != that[i + start]) {
                return i;
            }
        }
        return n;
    }

    public int encodeId(String value)
    {
        if (value == null) {
            return nullId;
        }
        return idNoNull(value.toCharArray());
    }

    private int idNoNull(char[] value)
    {
        if (value.length == 0) {
            return root.getId();
        }
        return id(root, value, 0);
    }

    private int id(TrieTreeNode curNode, char[] value, int start)
    {
        int pos = binarySearch(curNode.getChildren(), 0, curNode.getChildren().length - 1, value, start);
        if (pos >= 0) {
            TrieTreeNode sameNode = curNode.getChildren()[pos];
            int sameLength = sameLength(sameNode.getValue(), value, start);
            if (sameLength + start == value.length) {
                if (sameLength == sameNode.getValue().length) {
                    return sameNode.getId();
                }
                else {
                    return INVALID_ID;
                }
            }
            else {
                if (sameLength < sameNode.getValue().length) {
                    return INVALID_ID;
                }
                else {
                    return id(sameNode, value, start + sameLength);
                }
            }
        }
        return INVALID_ID;
    }

    @Override
    public int count()
    {
        return noNullValues.size() + (nullId == INVALID_ID ? 0 : 1);
    }

    @Override
    public int maxEncodeId()
    {
        return nextId - 1;
    }

    public int[] sortValue()
    {
        noNullValues.sort(String::compareTo);
        int[] ids = new int[noNullValues.size() + 1];
        sortNode(root, ids, 1);
        return ids;
    }

    public int[] quickSort()
    {
        noNullValues.sort(new Comparator<String>()
        {
            @Override
            public int compare(String a, String b)
            {
                return BufferUtil.compare(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
            }
        });
        int[] ids = new int[noNullValues.size() + 1];
        sortNode(root, ids, 1);
        return ids;
    }

    private int sortNode(TrieTreeNode node, int[] ids, int id)
    {
        for (TrieTreeNode child : node.getChildren()) {
            if (child.getId() > 0) {
                int oldId = child.getId();
                int newId = id++;
                ids[oldId] = newId;
                child.setId(newId);
            }
            id = sortNode(child, ids, id);
        }
        return id;
    }

    //@Override
    public int writeSst(StreamWriter output, VectorWriterFactory writerFactory)
            throws IOException
    {
        output.putByte(nullId);

        VectorWriterFactory vectorWriterFactory = new VectorWriterFactory(writerFactory.getDir(), writerFactory.getName(), "dict");
        CStoreColumnWriter<String> columnWriter = new BinaryOffsetColumnWriter<>(vectorWriterFactory, BufferCoder.UTF8, true);

        for (String val : noNullValues) {
            columnWriter.write(val);
        }
        columnWriter.flush();
        int valSize = columnWriter.appendTo(output);

        columnWriter.close();
        return Byte.BYTES + valSize;
    }

    public int writeTrieTree(StreamWriter output)
            throws IOException
    {
        output.putByte(nullId);
        int treeSize = writeTree(output);
        return Byte.BYTES + treeSize;
    }

    public String decodeValue(int id)
    {
        Preconditions.checkArgument(id >= 0);
        if (id == 0) {
            return null;
        }
        return noNullValues.get(id - getNonNullValueStartId());
    }

    private int writeTree(StreamWriter writer)
    {
        return writeTree(writer, 0, root);
    }

    private int writeTree(StreamWriter output, int offset, TrieTreeNode node)
    {
        int[] offsets = new int[node.getChildren().length];
        char[] firstChar = new char[node.getChildren().length];
        for (int i = 0; i < node.getChildren().length; i++) {
            TrieTreeNode child = node.getChildren()[i];
            firstChar[i] = child.getValue()[0];
            offset = writeTree(output, offset, child);
            offsets[i] = offset;
        }

        if (node.getValue().length == 0) {
            for (int i = 0; i < node.getChildren().length; i++) {
                output.putChar(firstChar[i]);
                output.putInt(offsets[i]);
            }
            output.putInt(node.getId());
            output.putInt(0);
            output.putInt(node.getChildren().length);

            int length = 3 * Integer.BYTES + node.getChildren().length * 6;
            return offset + length;
        }
        else {
            output.putCharArray(node.getValue(), 1, node.getValue().length);
            for (int i = 0; i < node.getChildren().length; i++) {
                output.putChar(firstChar[i]);
                output.putInt(offsets[i]);
            }
            output.putInt(node.getId());
            output.putInt(node.getValue().length - 1);
            output.putInt(node.getChildren().length);

            int length = 3 * Integer.BYTES + (node.getValue().length - 1) * 2 + node.getChildren().length * 6;
            return offset + length;
        }
    }

    public static MutableTrieTree create(SortedSet<String> values)
    {
        MutableTrieTree heapTree = new MutableTrieTree();
        values.forEach(heapTree::encode);
        return heapTree;
    }

    @Override
    public BufferComparator encodeComparator()
    {
        return new BufferComparator()
        {
            @Override
            public int compare(ByteBuffer a, int oa, ByteBuffer b, int ob)
            {
                return Integer.compare(a.getInt(oa), b.getInt(ob));
            }
        };
    }
}
