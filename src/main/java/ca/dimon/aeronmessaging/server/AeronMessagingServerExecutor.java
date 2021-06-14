package ca.dimon.aeronmessaging.server;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The default implementation of the {@link AeronMessagingServerExecutorService}
 * interface.
 */
public final class AeronMessagingServerExecutor implements AeronMessagingServerExecutorService {

    // All classes of this project should use the same log4j logger (and never use System.out.print() for loggign):
    protected final Logger logger = LogManager.getLogger(this.getClass());
    // see discussion: https://stackoverflow.com/questions/7624895/how-to-use-log4j-with-multiple-classes
    // in particular this answer: https://stackoverflow.com/a/7625722/7022062

    private final ExecutorService executor;

    private AeronMessagingServerExecutor(
            final ExecutorService in_exec) {
        this.executor = Objects.requireNonNull(in_exec, "exec");
    }

    @Override
    public boolean isExecutorThread() {
        return Thread.currentThread() instanceof ServerThread;
    }

    @Override
    public Future<Integer> my_call(final Callable<Integer> callable) {
        Objects.requireNonNull(callable, "callable");

        return this.executor.submit(() -> { // Dimon: Runnable tutorial: https://knpcode.com/java/concurrency/java-executor-tutorial-executorservice-scheduledexecutorservice/  submit() returns Future<?>
            
            // TODO: figure out why we can't use try-catch block here. Afterall "callable.call()" can throw and Exception.
            return callable.call();
            
//            try {
////        logger.info("+++ executor called on thread id: " + Thread.currentThread().getId() + ", thread name: " + Thread.currentThread().getName());
//                // Return received_fragments_count
//                //return callable.call();
//                return 1;
//            } catch (final Throwable e) {
//                logger.error("uncaught exception: ", e);
//            }
        });
    }

    @Override
    public void close() {
        this.executor.shutdown();
    }

    private static final class ServerThread extends Thread {

        ServerThread(final Runnable target) {
            super(Objects.requireNonNull(target, "target"));
        }
    }

    /**
     * @return A new executor
     */
    public static AeronMessagingServerExecutor create() {
        final ThreadFactory factory = r -> {
            final ServerThread t = new ServerThread(r);
            t.setName(new StringBuilder(64)
                    .append("ca.dimon.aeronmessaging.server[")
                    .append(Long.toUnsignedString(t.getId()))
                    .append("]")
                    .toString());
            return t;
        };

        return new AeronMessagingServerExecutor(Executors.newSingleThreadExecutor(factory));
    }
}
