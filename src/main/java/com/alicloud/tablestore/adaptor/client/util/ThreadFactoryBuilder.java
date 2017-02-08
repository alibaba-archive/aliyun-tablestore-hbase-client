package com.alicloud.tablestore.adaptor.client.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A ThreadFactory builder, providing any combination of these features:
 * <ul>
 * <li>whether threads should be marked as {@linkplain Thread#setDaemon daemon} threads
 * <li>a {@linkplain ThreadFactoryBuilder#setNameFormat naming format}
 * <li>a {@linkplain Thread#setPriority thread priority}
 * <li>an {@linkplain Thread#setUncaughtExceptionHandler uncaught exception handler}
 * <li>a {@linkplain ThreadFactory#newThread backing thread factory}
 * </ul>
 * If no backing thread factory is provided, a default backing thread factory is used as if by
 * calling {@code setThreadFactory(}{@link Executors#defaultThreadFactory()}{@code )}.
 * @author Kurt Alfred Kluever
 * @since 4.0
 */
public final class ThreadFactoryBuilder {
  private String nameFormat = null;
  private Boolean daemon = null;
  private Integer priority = null;
  private UncaughtExceptionHandler uncaughtExceptionHandler = null;
  private ThreadFactory backingThreadFactory = null;

  /**
   * Creates a new {@link ThreadFactory} builder.
   */
  public ThreadFactoryBuilder() {
  }

  /**
   * Sets the naming format to use when naming threads ({@link Thread#setName}) which are created
   * with this ThreadFactory.
   * @param nameFormat a {@link String#format(String, Object...)}-compatible format String, to which
   *          a unique integer (0, 1, etc.) will be supplied as the single parameter. This integer
   *          will be unique to the built instance of the ThreadFactory and will be assigned
   *          sequentially.
   * @return this for the builder pattern
   */
  public ThreadFactoryBuilder setNameFormat(String nameFormat) {
    String.format(nameFormat, 0); // fail fast if the format is bad or null
    this.nameFormat = nameFormat;
    return this;
  }

  /**
   * Sets daemon or not for new threads created with this ThreadFactory.
   * @param daemon whether or not new Threads created with this ThreadFactory will be daemon threads
   * @return this for the builder pattern
   */
  public ThreadFactoryBuilder setDaemon(boolean daemon) {
    this.daemon = daemon;
    return this;
  }

  /**
   * Sets the priority for new threads created with this ThreadFactory.
   * @param priority the priority for new Threads created with this ThreadFactory
   * @return this for the builder pattern
   */
  public ThreadFactoryBuilder setPriority(int priority) {
    this.priority = priority;
    return this;
  }

  /**
   * Sets the {@link UncaughtExceptionHandler} for new threads created with this ThreadFactory.
   * @param uncaughtExceptionHandler the uncaught exception handler for new Threads created with this
   *          ThreadFactory
   * @return this for the builder pattern
   */
  public ThreadFactoryBuilder setUncaughtExceptionHandler(
      UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    return this;
  }

  /**
   * Sets the backing {@link ThreadFactory} for new threads created with this ThreadFactory. Threads
   * will be created by invoking #newThread(Runnable) on this backing {@link ThreadFactory}.
   * @param backingThreadFactory the backing {@link ThreadFactory} which will be delegated to during
   *          thread creation.
   * @return this for the builder pattern
   */
  public ThreadFactoryBuilder setThreadFactory(ThreadFactory backingThreadFactory) {
    this.backingThreadFactory = backingThreadFactory;
    return this;
  }

  /**
   * Returns a new thread factory using the options supplied during the building process. After
   * building, it is still possible to change the options used to build the ThreadFactory and/or
   * build again. State is not shared amongst built instances.
   * @return the fully constructed {@link ThreadFactory}
   */
  public ThreadFactory build() {
    return build(this);
  }

  private static ThreadFactory build(ThreadFactoryBuilder builder) {
    final String nameFormat = builder.nameFormat;
    final Boolean daemon = builder.daemon;
    final Integer priority = builder.priority;
    final UncaughtExceptionHandler uncaughtExceptionHandler = builder.uncaughtExceptionHandler;
    final ThreadFactory backingThreadFactory =
        (builder.backingThreadFactory != null) ? builder.backingThreadFactory : Executors
            .defaultThreadFactory();
    final AtomicLong count = (nameFormat != null) ? new AtomicLong(0) : null;
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = backingThreadFactory.newThread(runnable);
        if (nameFormat != null) {
          thread.setName(String.format(nameFormat, count.getAndIncrement()));
        }
        if (daemon != null) {
          thread.setDaemon(daemon);
        }
        if (priority != null) {
          thread.setPriority(priority);
        }
        if (uncaughtExceptionHandler != null) {
          thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        return thread;
      }
    };
  }
}
