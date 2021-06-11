package github.cstore.io;

import com.google.common.io.Files;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Supplier;

public class FileStreamWriterFactory
        implements StreamWriterFactory
{
    private final File directory;

    public FileStreamWriterFactory(File directory)
    {
        this.directory = directory;
    }

    @Override
    public StreamWriter createWriter(String name, boolean clean)
    {
        File file = new File(directory, UUID.randomUUID().toString());
        if (file.exists()) {
            assert file.delete();
        }
        try {
            return new OutputStreamWriter(new DataOutputStream(new FileOutputStream(file)),
                    new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            if (clean) {
                                file.delete();
                            }
                        }
                    },
                    new Supplier<ByteBuffer>()
                    {
                        @Override
                        public ByteBuffer get()
                        {
                            try {
                                return Files.map(file);
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
