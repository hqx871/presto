package github.cstore.dictionary;

public interface Dictionary<T>
{
    int lookupId(String value);

    T lookupValue(int id);

    int count();

    int getMaxId();
}
