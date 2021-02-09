package com.sodonnell;

import com.sodonnell.exceptions.MisalignedBuffersException;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ECChecker {

  public static boolean validateParity(ByteBuffer[] buf, ErasureCodingPolicy ecPolicy)
      throws IOException {
    return validateParity(buf, ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits(), ecPolicy.getCellSize());
  }

  /**
   * This methods receives an array of byte buffers representing an Erasure Coded Stripe. It expects
   * the buffers to have a capacity of the EC cellsize, and the position in the buffers are such that the
   * data has just been read. The buffers will then be flipped so the current position becomes the limit.
   * Rhe data buffers will be used to generate the parity. The parity generated is then compared with
   * the parity in the passed buffers. If they match, return true, otherwise return false.
   *
   * Note that the buffers must also meet the requirements mentioned in validateBuffers.
   *
   * The buffers will be modified by this routine, as their limit will be set to the position
   * and the data buffers (except the first) may be padded with zeros to the first data buffers
   * original position.
   *
   * @param buf Buffers representing the EC stripe.
   * @param dataNum The EC Data Units
   * @param parityNum The EC Parity Units
   * @param cellSize The EC Cell Size
   * @return True if the generated parity matches the calculated parity.
   * @throws IOException
   */
  public static boolean validateParity(ByteBuffer[] buf, int dataNum, int parityNum, int cellSize)
     throws IOException {

    validateBuffers(buf, dataNum, parityNum, cellSize);
    padDataBuffers(buf, dataNum);
    ECValidateUtil.flipBuffers(buf);
    RSRawEncoder encoder = new RSRawEncoder(new ErasureCoderOptions(dataNum, parityNum));

    ByteBuffer[] inputs = new ByteBuffer[dataNum];
    System.arraycopy(buf, 0, inputs, 0, dataNum);
    ByteBuffer[] outputs = ECValidateUtil.allocateBuffers(parityNum, inputs[0].limit());

    encoder.encode(inputs, outputs);

    // After encoding, ensure that the generated parity matches those passed in buf
    for (int i=0; i<parityNum; i++) {
      if (outputs[i].compareTo(buf[dataNum+i]) != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check to see if any of the parity buffers contain only zero bytes for their entire length.
   * Return the number of parity buffers found to contain only zero bytes.
   * This method expects the buffer position to be at its limit, as if it had just be read
   * from the channel. On completion, the position will be at the limit.
   * @param buf
   * @param ecPolicy
   * @return
   * @throws IOException
   */
  public static Set<Integer> getNonZeroParityIndicies(ByteBuffer[] buf, ErasureCodingPolicy ecPolicy)
      throws IOException {
    return getNonZeroParityIndicies(buf, ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits(), ecPolicy.getCellSize());
  }

  public static Set<Integer> getNonZeroParityIndicies(ByteBuffer[] buf, int dataNum, int parityNum, int cellSize)
      throws IOException {
    validateBuffers(buf, dataNum, parityNum, cellSize);
    Set<Integer> nonZeroIndicies = new HashSet<>();
    for (int i=dataNum; i<dataNum+parityNum; i++) {
      ByteBuffer b = buf[i];
      b.flip();
      boolean hasContent = false;
      while (b.hasRemaining()) {
        if ((byte)0 != b.get()) {
          nonZeroIndicies.add(i);
          b.position(b.limit());
          break;
        }
      }
    }
    return nonZeroIndicies;
  }


  /**
   * Given a list of buffers representing some EC data, it is assumed the buffers have been filled
   * via a read from the datasource, and the buffer positions indicate what was read.
   *
   * The positions of the buffers in the input array also represent their position in an EC stripe.
   *
   * The first data buffer position indicates the position other buffers should be at.
   *
   * All parity buffers should be at the same position as the first data buffer.
   *
   * If the first data buffer is not full (ie its position is less than the cellSize) then all
   * other data buffers must be empty.
   *
   * If the first data buffer is full, then the next data buffer may be full or part filled. If
   * it is part filled, the remaining buffers must be empty and so on.
   *
   * @param buf
   * @param dataNum
   * @param parityNum
   * @param cellsize
   * @throws Exception
   */
  public static void validateBuffers(ByteBuffer[] buf, int dataNum, int parityNum, int cellsize)
      throws MisalignedBuffersException {
    int dataLimit = buf[0].position();
    for (int i=dataNum; i<dataNum+parityNum; i++) {
      if (buf[i].position() != dataLimit) {
        throw new MisalignedBuffersException("Parity buffer at index "+i+" should have a position equal to the first data buffer position");
      }
    }

    for (int i=1; i<dataNum; i++) {
      if (dataLimit < cellsize && buf[i].position() > 0) {
        throw new MisalignedBuffersException(
            "Data buffer at index " + i + " should have zero bytes as the buffer at index " + (i - 1) + " is less than the cell size");
      }
      dataLimit = buf[i].position();
    }
  }


  public static void padDataBuffers(ByteBuffer[] buf, int dataNum) {
    int dataLimit = buf[0].position();
    for (int i=1; i<dataNum; i++) {
      // Fill it to data limit with zeros
      ECValidateUtil.padBufferToLimit(buf[i], dataLimit);
    }
  }

}
