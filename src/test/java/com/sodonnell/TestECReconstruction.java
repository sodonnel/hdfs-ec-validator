package com.sodonnell;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestECReconstruction {

  private RSRawEncoder encoder;
  private RSRawDecoder decoder;
  private int parityUnits = 4;
  private int dataUnits = 10;
  private int bytesPerBuffer = 1024*1024;

  @Before
  public void setup() {
    encoder = new RSRawEncoder(new ErasureCoderOptions(dataUnits, parityUnits));
    decoder = new RSRawDecoder(new ErasureCoderOptions(dataUnits, parityUnits));
  }

  @After
  public void teardown() {
  }

  @Test
  /**
   * Prove a the encoder and decoder work in a happy path case.
   */
  public void testSimpleBlockRecovery() throws IOException {
    ByteBuffer[] data = generateDataBuffers();
    ByteBuffer[] parity = generateParityBuffers();

    encoder.encode(data, parity);
    // Set data 1 and 2 as missing, and parity 3 and 4 as not needed
    ByteBuffer[] recovered =
        reconstruct(data, parity, new int[]{0, 1}, new int[]{12, 13});

    // Check recovered 0 and 1 == data 0 and 1, proving recovery worked.
    assertEquals(0, data[0].compareTo(recovered[0]));
    assertEquals(0, data[1].compareTo(recovered[1]));
  }

  @Test
  /**
   * Here, we corrupt the first parity block with all zeros. Then
   * using that parity, corrupt the first data block.
   * Finally, using all the data blocks, gennerate the parity blocks.
   * In this case, parity[0] generates to all zeros, but parity[1-3] do not
   * match the original proving there is a corruption.
   */
  public void testBlockRecoveryUsingZeroedParity() throws IOException {
    ByteBuffer[] data = generateDataBuffers();
    ByteBuffer[] parity = generateParityBuffers();

    encoder.encode(data, parity);

    // Corrupt one parity by making it all zeros
    ByteBuffer originalParity = parity[0];
    parity[0] = ByteBuffer.allocate(bytesPerBuffer);
    fillBufferWithByte(parity[0], (byte)0);

    ByteBuffer[] recovered = reconstruct(data, parity, new int[]{0}, new int[]{11, 12, 13});

    // Check the recovered data is not equal to the original - it is now corrupted
    assertNotEquals(0, data[0].compareTo(recovered[0]));
    ByteBuffer originalData = data[0];
    data[0] = recovered[0];

    ByteBuffer[] newParity = reconstruct(data, parity, new int[]{10, 11, 12, 13}, new int[]{});

    assertEquals(0, newParity[0].compareTo(parity[0]));
    assertNotEquals(0, newParity[1].compareTo(parity[1]));
    assertNotEquals(0, newParity[2].compareTo(parity[2]));
    assertNotEquals(0, newParity[3].compareTo(parity[3]));
  }

  @Test
  /**
   * Here, we corrupt the first parity block with all zeros. Then
   * using that parity, regenerate the first 4 data blocks.
   * It is not possible detect this corruption as all remaining blocks
   * were used to recover the lost data block and hence all combination of
   * reconstruction will produce the same result.
   */
  public void testParityNumRecoveredBlocksUsingZeroedParity() throws IOException {
    ByteBuffer[] data = generateDataBuffers();
    ByteBuffer[] parity = generateParityBuffers();

    encoder.encode(data, parity);

    // Corrupt one parity by making it all zeros
    ByteBuffer originalParity = parity[0];
    parity[0] = ByteBuffer.allocate(bytesPerBuffer);
    fillBufferWithByte(parity[0], (byte)0);

    // Now reconstruct data[0-3] using the bad parity
    ByteBuffer[] recovered = reconstruct(data, parity, new int[]{0, 1, 2, 3}, new int[]{});

    // Check the recovered data is not equal to the original - it is now corrupted
    for (int i=0; i<4; i++) {
      assertNotEquals(0, data[i].compareTo(recovered[i]));
      data[i] = recovered[i];
    }
    ByteBuffer[] newParity = reconstruct(data, parity, new int[]{10, 11, 12, 13}, new int[]{});

    assertEquals(0, newParity[0].compareTo(parity[0]));
    assertEquals(0, newParity[1].compareTo(parity[1]));
    assertEquals(0, newParity[2].compareTo(parity[2]));
    assertEquals(0, newParity[3].compareTo(parity[3]));
  }

  @Test
  /**
   * Here, we corrupt the first parity block with all zeros. Then
   * using that parity, regenerate the first 4 data blocks.
   * It is not possible detect this corruption as all remaining blocks
   * were used to recover the lost data block and hence all combination of
   * reconstruction will produce the same result.
   */
  @Ignore // This test does not reflect the description above as it was later modified.
  public void testMultipleRecoverBlockUsingZeroedParity() throws IOException {
    ByteBuffer[] data = generateDataBuffers();
    ByteBuffer[] parity = generateParityBuffers();

    encoder.encode(data, parity);

    // Corrupt one parity by making it all zeros
    ByteBuffer originalParity = parity[0];
    parity[0] = ByteBuffer.allocate(bytesPerBuffer);
    fillBufferWithByte(parity[0], (byte)0);

    // Now reconstruct data[0] using the bad parity
    ByteBuffer[] recovered =
        reconstruct(data, parity, new int[]{0}, new int[]{11, 12, 13});
    data[0] = recovered[0];

    recovered = reconstruct(data, parity, new int[]{1}, new int[]{10, 12, 13});
    data[1] = recovered[0];

    recovered = reconstruct(data, parity, new int[]{2}, new int[]{10, 11, 12});
    data[2] = recovered[0];

    recovered = reconstruct(data, parity, new int[]{3}, new int[]{10, 11, 13});
    data[3] = recovered[0];

    ByteBuffer[] newParity =
        reconstruct(data, parity, new int[]{10, 11, 12, 13}, new int[]{});

    assertEquals(0, newParity[0].compareTo(parity[0]));
    assertEquals(0, newParity[1].compareTo(parity[1]));
    assertEquals(0, newParity[2].compareTo(parity[2]));
    assertEquals(0, newParity[3].compareTo(parity[3]));
  }

  private ByteBuffer[] generateDataBuffers() {
    ByteBuffer[] inputs = new ByteBuffer[dataUnits];
    for (int i=0; i<dataUnits; i++) {
      inputs[i] = ByteBuffer.allocate(bytesPerBuffer);
      inputs[i].put(RandomUtils.nextBytes(bytesPerBuffer));
      inputs[i].position(0);
    }
    return inputs;
  }

  private ByteBuffer[] generateParityBuffers() {
    return generateBuffersOfSize(parityUnits, bytesPerBuffer);
  }

  private ByteBuffer[] generateBuffersOfSize(int num, int size) {
    ByteBuffer[] outputs = new ByteBuffer[num];
    for (int i=0; i<num; i++) {
      outputs[i] = ByteBuffer.allocate(size);
    }
    return outputs;
  }

  private ByteBuffer[] reconstruct(ByteBuffer[] input, ByteBuffer[] parity,
                                   int[] recreate, int[] omit) throws IOException {
    int[] remove = new int[recreate.length + omit.length];
    System.arraycopy(recreate, 0, remove, 0, recreate.length);
    System.arraycopy(omit, 0, remove, recreate.length, omit.length);

    ByteBuffer[] toDecode = generateBuffersForRecovery(input, parity, remove);
    ByteBuffer[] recovered =
        generateBuffersOfSize(recreate.length, bytesPerBuffer);
    decoder.decode(toDecode, recreate, recovered);
    return recovered;
  }

  private ByteBuffer[] generateBuffersForRecovery(ByteBuffer[] data, ByteBuffer[] parity, int ...remove) {
    ByteBuffer[] outputs = new ByteBuffer[dataUnits+parityUnits];
    for (int i=0; i<dataUnits; i++) {
      outputs[i] = data[i];
      outputs[i].position(0);
    }
    for (int i=dataUnits; i<dataUnits+parityUnits; i++) {
      outputs[i] = parity[i-dataUnits];
      outputs[i].position(0);
    }
    for (int i : remove) {
      outputs[i] = null;
    }
    return outputs;
  }

  private void fillBufferWithByte(ByteBuffer buf, byte b) {
    while (buf.hasRemaining()) {
      buf.put(b);
    }
    buf.position(0);
  }

}

