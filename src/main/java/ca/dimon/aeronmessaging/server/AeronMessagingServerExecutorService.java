package ca.dimon.aeronmessaging.server;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * A simple executor service. {@link Runnable} values are executed in the order
 * that they are submitted on a single <i>executor thread</i>.
 */

public interface AeronMessagingServerExecutorService extends AutoCloseable//, Executor
{
  /**
   * @return {@code true} if the caller of this method is running on the executor thread
   */

  boolean isExecutorThread();

  /**
   * Raise {@link IllegalStateException} if {@link #isExecutorThread()} would
   * currently return {@code false}.
   */

  default void assertIsExecutorThread()
  {
    if (!this.isExecutorThread()) {
      throw new IllegalStateException(
        "The current thread is not a server executor thread");
    }
  }
  
  // It used to be "Executor.execute()" method here, but it was returning void (and taking Runnable), but 
  // we're trying to move from Runnable to Callable (so we can return Future<Integer> from subscription.poll() calls
  // in order to decide if we need to sleep 1ms in the main loop when no messages flying or we should keep busy
  // loop shoveling poll'ed messages if we received any fragments). TODO: find proper replacement for Executor.execute()
  // method, but for now let's add "home-made" shiny "my_call(Callable<Integer)" here - it does the job!-)
  Future<Integer> my_call(final Callable<Integer> callable);
}
