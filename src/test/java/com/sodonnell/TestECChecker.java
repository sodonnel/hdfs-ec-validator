package com.sodonnell;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static junit.framework.TestCase.assertEquals;

public class TestECChecker {

  private int dataUnits = 6;
  private int parityUnits = 3;
  private int cellSize = 1024*1024;

  @Before
  public void setup() {
  }

  @After
  public void teardown() {
  }

  @Test
  public void testValidParityIsSeenAsValid() throws IOException{
    RSRawEncoder encoder = new RSRawEncoder(new ErasureCoderOptions(dataUnits, parityUnits));
    ByteBuffer[] data = ECValidateUtil.allocateBuffers(dataUnits, cellSize);
    ByteBuffer[] parity = ECValidateUtil.allocateBuffers(parityUnits, cellSize);
    ByteBuffer[] stripe = new ByteBuffer[dataUnits + parityUnits];

    fillBuffersWithRandom(data);
    encoder.encode(data, parity);

    System.arraycopy(data, 0, stripe, 0, dataUnits);
    System.arraycopy(parity, 0, stripe, dataUnits, parityUnits);

    boolean res = ECChecker.validateParity(stripe, dataUnits, parityUnits, cellSize);
    assertEquals(true, res);
  }

  @Test
  public void testBadParityIsSeenAsInvalid() throws IOException{
    RSRawEncoder encoder = new RSRawEncoder(new ErasureCoderOptions(dataUnits, parityUnits));
    ByteBuffer[] data = ECValidateUtil.allocateBuffers(dataUnits, cellSize);
    ByteBuffer[] parity = ECValidateUtil.allocateBuffers(parityUnits, cellSize);
    ByteBuffer[] stripe = new ByteBuffer[dataUnits + parityUnits];

    fillBuffersWithRandom(data);
    encoder.encode(data, parity);

    // Change a single byte of one of the parity buffers
    byte existing = parity[0].get(0);
    parity[0].put(0, ++existing);

    System.arraycopy(data, 0, stripe, 0, dataUnits);
    System.arraycopy(parity, 0, stripe, dataUnits, parityUnits);

    boolean res = ECChecker.validateParity(stripe, dataUnits, parityUnits, cellSize);
    assertEquals(false, res);
  }

  private void fillBuffersWithRandom(ByteBuffer[] buf) {
    for (ByteBuffer b : buf) {
      b.position(0);
      b.put(RandomUtils.nextBytes(b.remaining()));
      b.position(0);
    }
  }

}
