package task4;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FixedThreadPoolExecutor implements WorkStealing.ShutdownableExecutor {
    private final ExecutorService pool;

    public FixedThreadPoolExecutor(int threads) {
        this.pool = Executors.newFixedThreadPool(threads);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }

    @Override
    public void shutdown() {
        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}