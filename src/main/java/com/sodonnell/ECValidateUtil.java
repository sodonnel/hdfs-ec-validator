package com.sodonnell;

import java.nio.ByteBuffer;

public class ECValidateUtil {

  public static ByteBuffer[] allocateBuffers(int count, int ofSize) {
    ByteBuffer[] buf = new ByteBuffer[count];
    for (int i=0; i<count; i++) {
      buf[i] = ByteBuffer.allocate(ofSize);
      buf[i].mark();
    }
    return buf;
  }

  public static void resetBufferPosition(ByteBuffer[] buf, int toPosition) {
    for (ByteBuffer b : buf) {
      b.position(toPosition);
    }
  }
}
