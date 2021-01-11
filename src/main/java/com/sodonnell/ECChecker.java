package com.sodonnell;

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ECChecker {

  public static boolean validateParity(ByteBuffer[] buf, ErasureCodingPolicy ecPolicy)
      throws IOException {
    return validateParity(buf, ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits(), ecPolicy.getCellSize());
  }
  public static boolean validateParity(ByteBuffer[] buf, int dataNum, int parityNum, int cellSize)
     throws IOException {

    RSRawEncoder encoder = new RSRawEncoder(new ErasureCoderOptions(dataNum, parityNum));

    ECValidateUtil.resetBufferPosition(buf, 0);
    ByteBuffer[] inputs = new ByteBuffer[dataNum];
    System.arraycopy(buf, 0, inputs, 0, dataNum);
    ByteBuffer[] outputs = ECValidateUtil.allocateBuffers(parityNum, cellSize);

    encoder.encode(inputs, outputs);

    // After encoding, ensure that the generated parity matches those passed in buf
    for (int i=0; i<parityNum; i++) {
      if (outputs[i].compareTo(buf[dataNum+i]) != 0) {
        return false;
      }
    }
    return true;
  }

}
