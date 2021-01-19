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

  public static void resetBufferPosition(ByteBuffer[] buf, int toPosition) {
    for (ByteBuffer b : buf) {
      b.position(toPosition);
    }
  }

  public static void zeroBuffers(ByteBuffer[] buf) {
    for (ByteBuffer b : buf) {
      zeroBuffer(b);
    }
  }

  public static void zeroBuffer(ByteBuffer buf) {
    Arrays.fill(buf.array(), (byte)0);
    buf.position(0);
  }
}
