package com.alicloud.tablestore.adaptor.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A factory for createing retry proxies
 */
public class RetryProxy {
  static final Log LOG = LogFactory.getLog(RetryProxy.class);

  public static <T> Object create(TablestoreClientConf conf) {
    return Proxy.newProxyInstance(conf.getClass().getClassLoader(),
      new Class<?>[] { OTSInterface.class }, new RetryInvocationHandler(conf));
  }

  /**
   * *Stop the proxy. Proxy must either implement {@link Closeable} or must have associated
   * {@link InvocationHandler}.
   * @param proxy
   */
  public static void stopProxy(Object proxy) {
    if (proxy == null) {
      return;
    }
    try {
      InvocationHandler handler = Proxy.getInvocationHandler(proxy);
      if (handler instanceof Closeable) {
        ((Closeable) handler).close();
        return;
      }
    } catch (IOException e) {
      LOG.error("Closing proxy or invocation handler caused exception", e);
    } catch (IllegalArgumentException e) {
      LOG.error("RetryProxy.stopProxy called on non proxy: class=" + proxy.getClass().getName(), e);
    }

  }
}
