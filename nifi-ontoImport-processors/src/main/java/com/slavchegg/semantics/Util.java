package com.slavchegg.semantics;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;


public class Util {

  public static <T> T inTx(GraphTraversalSource g, Callable<T> callable) {
    try {
      if (DEFAULT.isShutdown()){
        DEFAULT = createDefaultPool();
      }
      return inTxFuture(DEFAULT, g, callable).get();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Error executing in separate transaction: " + e.getMessage(), e);
    }
  }

  public static <T> Future<T> inTxFuture(ExecutorService pool, GraphTraversalSource g,
      Callable<T> callable) {
    try {
      return pool.submit(() -> {
        try (Transaction tx = g.tx()) {
          T result = callable.call();
          tx.commit();
          g.close();
          return result;
        }
      });

    } catch (Exception e) {
      throw new RuntimeException("Error executing in separate transaction", e);
    }
  }

  public static ExecutorService DEFAULT = createDefaultPool();

  public static ExecutorService createDefaultPool() {
    int threads = Runtime.getRuntime().availableProcessors() * 2;
    int queueSize = threads * 25;
    return new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(queueSize),
        new CallerBlocksPolicy());
  }

  static class CallerBlocksPolicy implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (!executor.isShutdown()) {
        // block caller for 100ns
        LockSupport.parkNanos(100);
        try {
          // submit again
          executor.submit(r).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
