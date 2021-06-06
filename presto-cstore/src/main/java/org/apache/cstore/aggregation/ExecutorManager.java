package org.apache.cstore.aggregation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorManager
{
    private final ExecutorService executor;

    public ExecutorManager()
    {
        int coreSize = Runtime.getRuntime().availableProcessors();
        this.executor = Executors.newFixedThreadPool(coreSize);
    }

    public ExecutorService getExecutor()
    {
        return executor;
    }
}
