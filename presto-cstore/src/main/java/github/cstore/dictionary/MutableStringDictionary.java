package github.cstore.dictionary;

public abstract class MutableStringDictionary
        extends StringDictionary
{
    public abstract int encode(String value);

    @Override
    public boolean isSort()
    {
        return false;
    }
}
