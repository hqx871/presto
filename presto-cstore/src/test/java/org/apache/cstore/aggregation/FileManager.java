package org.apache.cstore.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FileManager
{
    private final String nameFormat;
    private final File tmpDir;
    private AtomicInteger fileId;
    private final List<File> fileList;

    public FileManager(String nameFormat, File tmpDir)
    {
        this.nameFormat = nameFormat;
        this.tmpDir = tmpDir;
        this.fileList = new ArrayList<>();
        this.fileId = new AtomicInteger(0);
    }

    public File createFile()
    {
        String name = String.format(nameFormat, fileId.getAndIncrement());
        File newFile = new File(tmpDir, name);
        if (newFile.isFile() && newFile.exists()) {
            //throw new RuntimeException();
            newFile.delete();
        }
        try {
            newFile.createNewFile();
            //newFile.deleteOnExit();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        fileList.add(newFile);
        return newFile;
    }

    public void clear()
    {
        fileList.forEach(File::delete);
    }

    public List<File> getFileList()
    {
        return fileList;
    }
}
