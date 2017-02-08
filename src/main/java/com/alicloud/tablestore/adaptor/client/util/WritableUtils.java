/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.client.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableUtils {
  /*
   * Write a String as a Network Int n, followed by n Bytes Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   */
  public static void writeString(DataOutput out, String s) throws IOException {
    if (s != null) {
      byte[] buffer = s.getBytes("UTF-8");
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
    } else {
      out.writeInt(-1);
    }
  }

  /*
   * Read a String as a Network Int n, followed by n Bytes Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   */
  public static String readString(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == -1) return null;
    byte[] buffer = new byte[length];
    in.readFully(buffer); // could/should use readFully(buffer,0,length)?
    return new String(buffer, "UTF-8");
  }

  /**
   * Serializes an integer to a binary stream with zero-compressed encoding. For -120 <= i <= 127,
   * only one byte is used with the actual value. For other values of i, the first byte value
   * indicates whether the integer is positive or negative, and the number of bytes that follow. If
   * the first byte value v is between -121 and -124, the following integer is positive, with number
   * of bytes that follow are -(v+120). If the first byte value v is between -125 and -128, the
   * following integer is negative, with number of bytes that follow are -(v+124). Bytes are stored
   * in the high-non-zero-byte-first order.
   * @param stream Binary output stream
   * @param i Integer to be serialized
   * @throws java.io.IOException
   */
  public static void writeVInt(DataOutput stream, int i) throws IOException {
    writeVLong(stream, i);
  }

  /**
   * Serializes a long to a binary stream with zero-compressed encoding. For -112 <= i <= 127, only
   * one byte is used with the actual value. For other values of i, the first byte value indicates
   * whether the long is positive or negative, and the number of bytes that follow. If the first
   * byte value v is between -113 and -120, the following long is positive, with number of bytes
   * that follow are -(v+112). If the first byte value v is between -121 and -128, the following
   * long is negative, with number of bytes that follow are -(v+120). Bytes are stored in the
   * high-non-zero-byte-first order.
   * @param stream Binary output stream
   * @param i Long to be serialized
   * @throws java.io.IOException
   */
  public static void writeVLong(DataOutput stream, long i) throws IOException {
    if (i >= -112 && i <= 127) {
      stream.writeByte((byte) i);
      return;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    stream.writeByte((byte) len);

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      stream.writeByte((byte) ((i & mask) >> shiftbits));
    }
  }

  /**
   * Reads a zero-compressed encoded long from input stream and returns it.
   * @param stream Binary input stream
   * @throws java.io.IOException
   * @return deserialized long from stream.
   */
  public static long readVLong(DataInput stream) throws IOException {
    byte firstByte = stream.readByte();
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len - 1; idx++) {
      byte b = stream.readByte();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * Parse the first byte of a vint/vlong to determine the number of bytes
   * @param value the first byte of the vint/vlong
   * @return the total number of bytes (1 to 9)
   */
  public static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  /**
   * Given the first byte of a vint/vlong, determine the sign
   * @param value the first byte
   * @return is the value negative
   */
  public static boolean isNegativeVInt(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * Reads a zero-compressed encoded integer from input stream and returns it.
   * @param stream Binary input stream
   * @throws java.io.IOException
   * @return deserialized integer from stream.
   */
  public static int readVInt(DataInput stream) throws IOException {
    long n = readVLong(stream);
    if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
      throw new IOException("value too long to fit in integer");
    }
    return (int) n;
  }

  /**
   * Get the encoded length if an integer is stored in a variable-length format
   * @return the encoded length
   */
  public static int getVIntSize(long i) {
    if (i >= -112 && i <= 127) {
      return 1;
    }

    if (i < 0) {
      i ^= -1L; // take one's complement'
    }
    // find the number of bytes with non-leading zeros
    int dataBits = Long.SIZE - Long.numberOfLeadingZeros(i);
    // find the number of data bytes + length byte
    return (dataBits + 7) / 8 + 1;
  }
}
