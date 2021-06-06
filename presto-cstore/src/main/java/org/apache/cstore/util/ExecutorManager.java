package org.apache.cstore.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorManager
{
    private final ExecutorService executor;

    public ExecutorManager(String nameFormat)
    {
        int coreSize = Runtime.getRuntime().availableProcessors();

        executor = new ThreadPoolExecutor(coreSize, coreSize, 1, TimeUnit.MINUTES,
                new LinkedBlockingDeque<>(),
                new ThreadFactoryBuilder().setNameFormat(nameFormat).build());
    }

    public ExecutorService getExecutor()
    {
        return executor;
    }
}
