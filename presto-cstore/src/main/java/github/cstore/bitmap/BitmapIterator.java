package github.cstore.bitmap;

public interface BitmapIterator
{
    boolean hasNext();

    int next(int[] mask);
}
