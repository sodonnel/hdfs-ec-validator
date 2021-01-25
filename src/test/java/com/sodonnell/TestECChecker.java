package com.sodonnell;

import com.sodonnell.exceptions.MisalignedBuffersException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

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
    ECValidateUtil.flipBuffers(data);
    encoder.encode(data, parity);

    for (ByteBuffer b : parity) {
      b.position(data[0].position());
    }

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
    ECValidateUtil.flipBuffers(data);
    encoder.encode(data, parity);

    for (ByteBuffer b : parity) {
      b.position(data[0].position());
    }

    // Change a single byte of one of the parity buffers
    byte existing = parity[0].get(0);
    parity[0].put(0, ++existing);

    System.arraycopy(data, 0, stripe, 0, dataUnits);
    System.arraycopy(parity, 0, stripe, dataUnits, parityUnits);

    boolean res = ECChecker.validateParity(stripe, dataUnits, parityUnits, cellSize);
    assertEquals(false, res);
  }

  @Test
  public void testAllZeroParity() throws IOException {
    ByteBuffer[] stripe = ECValidateUtil.allocateBuffers(dataUnits + parityUnits, cellSize);
    fillBuffersWithRandom(stripe);
    assertEquals(0, ECChecker.allZeroParity(stripe, dataUnits, parityUnits, cellSize));
    for (int i=dataUnits; i<dataUnits+parityUnits; i++) {
      assertEquals(stripe[i].limit(), stripe[i].position());
    }

    // Zero the second parity buffer
    ByteBuffer secondParity = stripe[dataUnits+1];
    ECValidateUtil.zeroBuffer(secondParity);
    secondParity.position(secondParity.limit());
    assertEquals(1, ECChecker.allZeroParity(stripe, dataUnits, parityUnits, cellSize));
    for (int i=dataUnits; i<dataUnits+parityUnits; i++) {
      assertEquals(stripe[i].limit(), stripe[i].position());
    }

    // Zero the third parity buffer
    ByteBuffer thirdParity = stripe[dataUnits+2];
    ECValidateUtil.zeroBuffer(thirdParity);
    thirdParity.position(thirdParity.limit());
    assertEquals(2, ECChecker.allZeroParity(stripe, dataUnits, parityUnits, cellSize));
    for (int i=dataUnits; i<dataUnits+parityUnits; i++) {
      assertEquals(stripe[i].limit(), stripe[i].position());
    }
  }

  @Test
  public void testValidateBuffers() throws Exception {
    ByteBuffer[] stripe = ECValidateUtil.allocateBuffers(dataUnits + parityUnits, cellSize);
    fillBuffersWithRandom(stripe);

    // Start with a full stripe. Position at capacity for all
    ECChecker.validateBuffers(stripe, dataUnits, parityUnits, cellSize);

    // Reset the position of any parity to less than the first data
    stripe[dataUnits].position(0);
    try {
      ECChecker.validateBuffers(stripe, dataUnits, parityUnits, cellSize);
      fail("Expected exception");
    } catch (MisalignedBuffersException e) {
      assertTrue(e.getMessage().matches("^Parity buffer at index 6.*"));
    }

    // Set the position of all buffers to less than cell size, make parity 1 greater
    for (ByteBuffer b : stripe) {
      b.position(100);
    }
    stripe[dataUnits].position(101);
    try {
      ECChecker.validateBuffers(stripe, dataUnits, parityUnits, cellSize);
      fail("Expected exception");
    } catch (MisalignedBuffersException e) {
      assertTrue(e.getMessage().matches("^Parity buffer at index 6.*"));
    }

    // Make it one less
    stripe[dataUnits].position(99);
    try {
      ECChecker.validateBuffers(stripe, dataUnits, parityUnits, cellSize);
      fail("Expected exception");
    } catch (MisalignedBuffersException e) {
      assertTrue(e.getMessage().matches("^Parity buffer at index 6.*"));
    }

    // Set the position of all buffers to less than cell size
    for (ByteBuffer b : stripe) {
      b.position(100);
    }
    try {
      ECChecker.validateBuffers(stripe, dataUnits, parityUnits, cellSize);
      fail("Expected exception");
    } catch (MisalignedBuffersException e) {
      assertTrue(e.getMessage().matches("^Data buffer at index 1.*"));
    }

    // set the second data buffer to zero as expected, but the third is still at 100
    stripe[1].position(0);
    try {
      ECChecker.validateBuffers(stripe, dataUnits, parityUnits, cellSize);
      fail("Expected exception");
    } catch (MisalignedBuffersException e) {
      assertTrue(e.getMessage().matches("^Data buffer at index 2.*"));
    }

    // First two data full, 3rd partial, 4th not at zero
    for (ByteBuffer b : stripe) {
      b.position(cellSize);
    }
    stripe[2].position(100);
    try {
      ECChecker.validateBuffers(stripe, dataUnits, parityUnits, cellSize);
      fail("Expected exception");
    } catch (MisalignedBuffersException e) {
      assertTrue(e.getMessage().matches("^Data buffer at index 3.*"));
    }
  }

  @Test
  public void testPadBuffers() throws Exception {
    ByteBuffer[] stripe = ECValidateUtil.allocateBuffers(dataUnits + parityUnits, cellSize);
    fillBuffersWithRandom(stripe);
    ECChecker.padDataBuffers(stripe, dataUnits);
    ECValidateUtil.flipBuffers(stripe);
    for (ByteBuffer b : stripe) {
      assertEquals(0, b.position());
      assertEquals(cellSize, b.remaining());
    }

    // Reset all buffers, still filled with random bytes
    ECValidateUtil.clearBuffers(stripe);
    // Set the first to position 100
    stripe[0].position(100);
    // Also set the parity to 100
    for (int i=dataUnits; i< dataUnits+parityUnits; i++) {
      stripe[i].position(100);
    }

    ECChecker.padDataBuffers(stripe, dataUnits);
    ECValidateUtil.flipBuffers(stripe);
    for (ByteBuffer b : stripe) {
      assertEquals(0, b.position());
      assertEquals(100, b.remaining());
    }
    // Data should have been padded with zeros, where it was random before
    for (int i=1; i<dataUnits; i++) {
      while(stripe[i].hasRemaining()) {
        assertEquals((byte)0, stripe[i].get());
      }
    }
  }

  @Test
  public void testBuffersNotFull() throws IOException{
    RSRawEncoder encoder = new RSRawEncoder(new ErasureCoderOptions(dataUnits, parityUnits));
    ByteBuffer[] data = ECValidateUtil.allocateBuffers(dataUnits, cellSize);
    ByteBuffer[] parity = ECValidateUtil.allocateBuffers(parityUnits, cellSize);
    ByteBuffer[] stripe = new ByteBuffer[dataUnits + parityUnits];

    // Populate the buffers with random and then clear - this creates dirty buffers
    fillBuffersWithRandom(data);
    fillBuffersWithRandom(parity);
    ECValidateUtil.clearBuffers(data);
    ECValidateUtil.clearBuffers(parity);

    // Now we set data[0] to have 100 bytes, but the other buffers need
    // to be set back to zero bytes.
    fillBufferWithRandom(data[0], 100);
    ECChecker.padDataBuffers(data, data.length);
    ECValidateUtil.flipBuffers(data);
    // The EC Encoder expects the parity buffers to have a limit of 100 bytes too.
    for (ByteBuffer b : parity) {
      b.limit(data[0].limit());
    }
    encoder.encode(data, parity);

    // The generated parity buffers always have their position at zero, and
    // limit at the number of generated bytes. We need to change the position
    // back to "remaining" so it looks like the parity was just read into the buffer
    for (ByteBuffer b : parity) {
      assertEquals(0, b.position());
      assertEquals(100, b.remaining());
    }

    // Data buffers limit should be unchanged, but as they have been read, the
    // position will have been advanced to the limit.
    for (ByteBuffer b : data) {
      assertEquals(100, b.position());
      assertEquals(0, b.remaining());
    }

    System.arraycopy(data, 0, stripe, 0, dataUnits);
    System.arraycopy(parity, 0, stripe, dataUnits, parityUnits);

    resetBuffers(stripe, 100);
    boolean res = ECChecker.validateParity(stripe, dataUnits, parityUnits, cellSize);
    assertEquals(true, res);

    // Change a single byte of one of the parity buffers
    byte existing = parity[0].get(0);
    parity[0].put(0, ++existing);

    resetBuffers(stripe, 100);
    res = ECChecker.validateParity(stripe, dataUnits, parityUnits, cellSize);
    assertEquals(false, res);
  }

  private void resetBuffers(ByteBuffer[] buf, int totalBytes) {
    buf[0].position(totalBytes);
    for (int i=1; i<dataUnits; i++) {
      buf[i].position(0);
    }
    for (int i=dataUnits; i<dataUnits+parityUnits; i++) {
      buf[i].position(totalBytes);
    }
  }

  private void fillBuffersWithRandom(ByteBuffer[] buf) {
    for (ByteBuffer b : buf) {
      b.position(0);
      b.put(RandomUtils.nextBytes(b.remaining()));
    }
  }

  private void fillBufferWithRandom(ByteBuffer buf, int bytes) {
    buf.put(RandomUtils.nextBytes(bytes));
  }

}
