package com.alicloud.tablestore.adaptor.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alicloud.tablestore.adaptor.DoNotRetryIOException;

/**
 * An invocation handler which supports retry for failed invoke.
 */
public class RetryInvocationHandler implements InvocationHandler, Closeable {
  public static final Log LOG = LogFactory.getLog(RetryInvocationHandler.class);

  private int retryCount = 0;
 
  private ThreadPoolExecutor pool;

  OTSImplement otsImplement = null;
  
  private TablestoreClientConf conf;
  
  protected RetryInvocationHandler(TablestoreClientConf conf) {
    this.conf = conf;
    this.retryCount = conf.getRetryCount();
    otsImplement = new OTSImplement(conf);
    this.pool =
        new ThreadPoolExecutor(1, Integer.MAX_VALUE, 60, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(), new DaemonThreadFactory());
    this.pool.allowCoreThreadTimeOut(true);
  }

  @Override
  public void close() throws IOException {
    if (this.pool != null) {
      this.pool.shutdown();
      this.pool = null;
    }
    if (this.otsImplement != null) {
      this.otsImplement.close();
    }
  }

  private class Caller implements Callable<Object> {
    private AtomicBoolean callInterrupted = new AtomicBoolean(false);;
    private final Method method;
    private final Object[] args;

    Caller(final Method method, final Object[] args) {
      this.method = method;
      this.args = args;
    }

    public void interrupted() {
      callInterrupted.set(true);
    }

    @Override
    public Object call() throws Exception {
      // The number of times this method invocation has been failed over.
      int invocationRetryCount = 0;
      Throwable error = null;
      while (true) {
        try {
          ClientFaultInjector.instance.fetchFromHBaseServiceException();
          Object ret = invokeMethod(otsImplement, method, args);
          return ret;
        } catch (Throwable e) {
          LOG.warn("Failed invoking method " + method.getName() + " to " + otsImplement
              + " because of " + e + ", will retry...");
          if (e instanceof DoNotRetryIOException) {
            throw new IOException(e);
          }
          error = e;
        }

        if (callInterrupted.get()) {
          LOG.debug("Calling " + method.getName() + " is interrupted...");
          throw new InterruptedException("Interrupted!");
        }

        // there is error, so retry the method
        if (invocationRetryCount >= retryCount) {
          String msg =
              "Failed calling " + method.getName() + " after retring " + invocationRetryCount
                  + " time(s)";
          LOG.warn(msg, error);
          throw new IOException(msg, error);
        }

        boolean isMethodIdempotent =
            com.alicloud.tablestore.adaptor.client.OTSInterface.class.getMethod(method.getName(), method.getParameterTypes())
                .isAnnotationPresent(Idempotent.class);

        if (isMethodIdempotent) {
          // if the method is idempotent, get one service instance from
          // cache and retry the method
          Thread.sleep(10 + invocationRetryCount * 1000);
          invocationRetryCount++;
        } else {
          throw new IOException(error);
        }
        error = null;
      }
    }
  };

  @Override
  public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {

    Caller callable = new Caller(method, args);

    if (conf.getOperationTimeout() == Integer.MAX_VALUE) {
      // Call directly if no operation timeout
      return callable.call();
    } else if (this.pool.getPoolSize() >= 500) {
      // Call directly if too many threads running in pool(It seems has thread
      // leak).
      LOG.warn("Too many threads(" + this.pool.getPoolSize()
          + ") running in the pool, dose it have thread leak?");
      return callable.call();
    } else {
      Future<Object> future = this.pool.submit(callable);
      try {
        return future.get(conf.getOperationTimeout(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new IOException("Interruppted");
      } catch (ExecutionException e) {
        throw new IOException(e);
      } catch (TimeoutException e) {
        callable.interrupted();
        future.cancel(true);
        throw new com.alicloud.tablestore.adaptor.client.OperationTimeoutException("Failed exectuing operation " + method.getName()
            + " after " + conf.getOperationTimeout() + "ms");
      }
    }
  }

  protected Object invokeMethod(com.alicloud.tablestore.adaptor.client.OTSInterface service, Method method, Object[] args)
      throws Throwable {
    try {
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      return method.invoke(service, args);
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }

  static class DaemonThreadFactory implements ThreadFactory {
    static final AtomicInteger poolNumber = new AtomicInteger(1);
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;

    DaemonThreadFactory() {
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      namePrefix = "otsadapter-operation-pool" + poolNumber.getAndIncrement() + "-thread-";
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      if (!t.isDaemon()) {
        t.setDaemon(true);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }

}
