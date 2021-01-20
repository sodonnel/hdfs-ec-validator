package com.sodonnell;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ECValidateUtil {

  public static ByteBuffer[] allocateBuffers(int count, int ofSize) {
    ByteBuffer[] buf = new ByteBuffer[count];
    for (int i=0; i<count; i++) {
      buf[i] = ByteBuffer.allocate(ofSize);
    }
    return buf;
  }

  public static void clearBuffers(ByteBuffer[] buf) {
    for (ByteBuffer b : buf) {
      b.clear();
    }
  }

  public static void flipBuffers(ByteBuffer[] buf) {
    for (ByteBuffer b : buf) {
      b.flip();
    }
  }

  public static void resetBufferPosition(ByteBuffer[] buf, int toPosition) {
    for (ByteBuffer b : buf) {
      b.position(toPosition);
    }
  }

  public static void padBufferToLimit(ByteBuffer buf, int limit) {
    int pos = buf.position();
    if (pos >= limit) {
      return;
    }
    Arrays.fill(buf.array(), pos, limit, (byte)0);
    buf.position(limit);
  }

  public static void zeroBuffers(ByteBuffer[] buf) {
    for (ByteBuffer b : buf) {
      zeroBuffer(b);
    }
  }

  public static void zeroBuffer(ByteBuffer buf) {
    Arrays.fill(buf.array(), (byte)0);
    buf.clear();
  }
}
