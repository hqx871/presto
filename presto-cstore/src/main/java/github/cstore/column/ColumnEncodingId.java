package github.cstore.column;

public final class ColumnEncodingId
{
    public static final byte PLAIN_BYTE = 1;
    public static final byte PLAIN_SHORT = 2;
    public static final byte PLAIN_INT = 3;
    public static final byte PLAIN_LONG = 4;
    public static final byte PLAIN_FLOAT = 5;
    public static final byte PLAIN_DOUBLE = 6;

    public static final byte STRING_BYTE = 7;
    public static final byte STRING_SHORT = 8;
    public static final byte STRING_INT = 9;

    public static final byte BINARY_OFFSET = 10;
    public static final byte BINARY_FIX = 11;
    public static final byte RLE_BYTE = 12;

    private ColumnEncodingId()
    {
    }
}
