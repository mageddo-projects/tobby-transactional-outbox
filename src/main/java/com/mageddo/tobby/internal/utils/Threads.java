package com.mageddo.tobby.internal.utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

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

  public static <T, R> List<R> executeAndGet(
      ExecutorService pool, List<T> objects, Function<T, R> converter
  ) {
    final List<Future<R>> futures = objects.stream()
        .map(it -> pool.submit(() -> converter.apply(it)))
        .collect(Collectors.toList());
    return get(futures);
  }

  /**
   * Submit all callables at the thread pool and wait all the futures to return.
   */
  public static <V> List<V> executeAndGet(ExecutorService executorService, List<Callable<V>> callables) {
    final List<Future<V>> futures = new ArrayList<>();
    for (Callable<V> callable : callables) {
      futures.add(executorService.submit(callable));
    }
    return get(futures);
  }

  /**
   * Wait all the futures to return.
   */
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

  /**
   * Sleep for some duration.
   *
   * @throws UncheckedInterruptedException when thread is interrupted.
   */
  public static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e);
    }
  }
}
