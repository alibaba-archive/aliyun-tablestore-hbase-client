/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.client;

import java.io.IOException;
import java.util.Iterator;

/**
 * Helper class for custom client scanners of {@link com.alicloud.tablestore.adaptor.struct.OResult}.
 */
public abstract class AbstractOClientScanner implements OResultScanner {

  public Iterator<com.alicloud.tablestore.adaptor.struct.OResult> iterator() {
    return new Iterator<com.alicloud.tablestore.adaptor.struct.OResult>() {
      // The next RowResult, possibly pre-read
      com.alicloud.tablestore.adaptor.struct.OResult next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      public boolean hasNext() {
        if (next == null) {
          try {
            next = AbstractOClientScanner.this.next();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      public com.alicloud.tablestore.adaptor.struct.OResult next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        com.alicloud.tablestore.adaptor.struct.OResult temp = next;
        next = null;
        return temp;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

}
