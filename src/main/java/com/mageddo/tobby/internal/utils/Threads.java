package com.mageddo.tobby.internal.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Threads {
  public static ExecutorService newPool(int size) {
    return Executors.newFixedThreadPool(size, r -> {
      Thread t = Executors
          .defaultThreadFactory()
          .newThread(r);
      t.setDaemon(true);
      return t;
    });
  }

  public static <V> List<V> executeAndGet(ExecutorService executorService, List<Callable<V>> callables) {
    final List<Future<V>> futures = new ArrayList<>();
    for (Callable<V> callable : callables) {
      futures.add(executorService.submit(callable));
    }
    return get(futures);
  }

  public static <V> List<V> get(List<Future<V>> futures) {
    final List<V> results = new ArrayList<>();
    for (Future<V> future : futures) {
      try {
        results.add(future.get());
      } catch (InterruptedException e) {
        throw new UncheckedInterruptedException(e);
      } catch (ExecutionException e) {
        throw new UncheckedExecutionException(e);
      }
    }
    return results;
  }
}
