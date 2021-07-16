package com.facebook.presto.cstore.storage;

import java.util.NoSuchElementException;

public enum ActionType
{
    APPEND(1),
    OVERWRITE(2),
    //DELETE(3),
    //UPDATE(4),
    CREATE(5),
    COMMIT(9),
    ROLLBACK(10),
    DROP(11);

    private final int code;

    ActionType(int code) {this.code = code;}

    public int getCode()
    {
        return code;
    }

    public static ActionType valueOfCode(int code)
    {
        switch (code) {
            case 1:
                return APPEND;
            case 2:
                return OVERWRITE;
            case 5:
                return CREATE;
            case 9:
                return COMMIT;
            case 10:
                return ROLLBACK;
            case 11:
                return DROP;
            default:
        }
        throw new NoSuchElementException();
    }
}
