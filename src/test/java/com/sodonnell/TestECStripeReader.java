package com.sodonnell;

import com.sodonnell.exceptions.BlockUnavailableException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

public class TestECStripeReader {

  private Logger LOG = LoggerFactory.getLogger(TestECStripeReader.class);

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DFSClient client;
  private String policyName = "RS-6-3-1024k";
  private ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getByName(policyName);
  private DistributedFileSystem fs;
  private Path ecRoot = new Path("/ecfiles");

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    // Avoid EC files failing to completely write due to load on the mini-cluster
    conf.setBoolean("dfs.namenode.redundancy.considerLoad", false);
    int numDataNodes = ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(policyName);
    client = new DFSClient(fs.getUri(), conf);
    fs.mkdirs(ecRoot);
    fs.setErasureCodingPolicy(ecRoot, policyName);
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFileWithExactStripeCanBeValidated() throws IOException {
    Path ecFile = new Path(ecRoot, "ecFile");
    // write one full stripe
    int bytes = ecPolicy.getNumDataUnits() * ecPolicy.getCellSize();
    createFileOfLength(ecFile, bytes);

    LocatedBlocks blocks = client.getNamenode().getBlockLocations("/ecfiles/ecFile", 0, bytes);
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) blocks.getLocatedBlocks().get(0);

    ECStripeReader ecStripeReader = new ECStripeReader(client, conf);
    ByteBuffer[] buffers = ecStripeReader.readStripe(blockGroup, ecPolicy, "/ecfiles/ecFile");
    assertEquals(buffers.length, ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());

    ECValidateUtil.resetBufferPosition(buffers, 0);
    for (int i=0; i<ecPolicy.getNumDataUnits(); i++) {
      FSDataInputStream out = fs.open(ecFile);
      ByteBuffer fbuf = ByteBuffer.allocate(ecPolicy.getCellSize());
      out.read(i*ecPolicy.getCellSize(), fbuf);
      fbuf.position(0);
      out.close();
      assertEquals(0, fbuf.compareTo(buffers[i]));
    }
  }

  @Test
  public void testFileWithMultipleStripesCanBeValidated() throws IOException {
    Path ecFile = new Path(ecRoot, "ecFile");
    // write 5 full stripes plus 1 byte
    int bytes = ecPolicy.getNumDataUnits() * ecPolicy.getCellSize() * 5 + 1;
    createFileOfLength(ecFile, bytes);

    LocatedBlocks blocks = client.getNamenode().getBlockLocations("/ecfiles/ecFile", 0, bytes);
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) blocks.getLocatedBlocks().get(0);

    ECStripeReader ecStripeReader = new ECStripeReader(client, conf);
    ByteBuffer[] buffers = ecStripeReader.readStripe(blockGroup, ecPolicy, "/ecfiles/ecFile");
    assertEquals(buffers.length, ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());

    ECValidateUtil.resetBufferPosition(buffers, 0);
    for (int i=0; i<ecPolicy.getNumDataUnits(); i++) {
      FSDataInputStream out = fs.open(ecFile);
      ByteBuffer fbuf = ByteBuffer.allocate(ecPolicy.getCellSize());
      out.read(i*ecPolicy.getCellSize(), fbuf);
      fbuf.position(0);
      out.close();
      assertEquals(0, fbuf.compareTo(buffers[i]));
    }
  }

  @Test
  public void testStripeWithLessThanCellSizeData() throws IOException {
    Path ecFile = new Path(ecRoot, "ecFile");
    // Going to write less than one cell.
    int bytes = 1024;
    createFileOfLength(ecFile, bytes);

    LocatedBlocks blocks = client.getNamenode().getBlockLocations("/ecfiles/ecFile", 0, bytes);
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) blocks.getLocatedBlocks().get(0);

    // The size of the Striped Block should be reported as the data length within it
    assertEquals(bytes, blockGroup.getBlockSize());

    ECStripeReader ecStripeReader = new ECStripeReader(client, conf);
    ByteBuffer[] buffers = ecStripeReader.readStripe(blockGroup, ecPolicy, "/ecfiles/ecFile");

    // We expect to receive a full set of buffers. However only the first buffer,
    // and the parity buffers will have any data in them. The others should have
    // a position of zero. All parity should be the same size as the data buffer.
    assertEquals(buffers.length, ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
    assertEquals(1024, buffers[0].position());
    assertEquals(0, buffers[1].position());
    assertEquals(0, buffers[2].position());
    assertEquals(0, buffers[3].position());
    assertEquals(0, buffers[4].position());
    assertEquals(0, buffers[5].position());
    assertEquals(1024, buffers[6].position());
    assertEquals(1024, buffers[7].position());
    assertEquals(1024, buffers[8].position());

    RSRawEncoder encoder = new RSRawEncoder(new ErasureCoderOptions(ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits()));

    ByteBuffer[] encoded = ECValidateUtil.allocateBuffers(ecPolicy.getNumParityUnits(), ecPolicy.getCellSize());
    ByteBuffer[] input = new ByteBuffer[ecPolicy.getNumDataUnits()];
    for (int i=0; i< ecPolicy.getNumDataUnits(); i++) {
      input[i] = buffers[i];
    }
    ECValidateUtil.resetBufferPosition(input, 0);
    encoder.encode(input, encoded);

    ECValidateUtil.resetBufferPosition(buffers, 0);
    assertEquals(0, encoded[0].compareTo(buffers[6]));
    assertEquals(0, encoded[1].compareTo(buffers[7]));
    assertEquals(0, encoded[2].compareTo(buffers[8]));
  }

  @Test
  public void testStripeWithMoreThanCellSizeButLessThanStripeSizeData() throws IOException {
    Path ecFile = new Path(ecRoot, "ecFile");
    // 2 full cells plus one byte
    int bytes = ecPolicy.getCellSize()*2+1;
    createFileOfLength(ecFile, bytes);

    LocatedBlocks blocks = client.getNamenode().getBlockLocations("/ecfiles/ecFile", 0, bytes);
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) blocks.getLocatedBlocks().get(0);

    // The size of the Striped Block should be reported as the data length within it
    assertEquals(bytes, blockGroup.getBlockSize());

    ECStripeReader ecStripeReader = new ECStripeReader(client, conf);
    ByteBuffer[] buffers = ecStripeReader.readStripe(blockGroup, ecPolicy, "/ecfiles/ecFile");

    // We expect to receive a full set of buffers. However only the first buffer,
    // and the parity buffers will have any data in them. The others should have
    // a position of zero. All parity should be the same size as the data buffer.
    assertEquals(buffers.length, ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
    assertEquals(ecPolicy.getCellSize(), buffers[0].position());
    assertEquals(ecPolicy.getCellSize(), buffers[1].position());
    assertEquals(1, buffers[2].position());
    assertEquals(0, buffers[3].position());
    assertEquals(0, buffers[4].position());
    assertEquals(0, buffers[5].position());
    assertEquals(ecPolicy.getCellSize(), buffers[6].position());
    assertEquals(ecPolicy.getCellSize(), buffers[7].position());
    assertEquals(ecPolicy.getCellSize(), buffers[8].position());

    RSRawEncoder encoder = new RSRawEncoder(new ErasureCoderOptions(ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits()));

    ByteBuffer[] encoded = ECValidateUtil.allocateBuffers(ecPolicy.getNumParityUnits(), ecPolicy.getCellSize());
    ByteBuffer[] input = new ByteBuffer[ecPolicy.getNumDataUnits()];
    for (int i=0; i< ecPolicy.getNumDataUnits(); i++) {
      input[i] = buffers[i];
    }
    ECValidateUtil.resetBufferPosition(input, 0);
    encoder.encode(input, encoded);
    ECValidateUtil.resetBufferPosition(encoded, 0);
    ECValidateUtil.resetBufferPosition(buffers, 0);

    assertEquals(0, encoded[0].compareTo(buffers[6]));
    assertEquals(0, encoded[1].compareTo(buffers[7]));
    assertEquals(0, encoded[2].compareTo(buffers[8]));
  }

  @Test
  public void testFileWithUnavailableParityBlockThrows() throws Exception {
    Path ecFile = new Path(ecRoot, "ecFile");
    // write one full stripe
    int bytes = ecPolicy.getNumDataUnits() * ecPolicy.getCellSize();
    createFileOfLength(ecFile, bytes);

    LocatedBlocks blocks = client.getNamenode().getBlockLocations("/ecfiles/ecFile", 0, bytes);
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) blocks.getLocatedBlocks().get(0);

    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(
        blockGroup, ecPolicy.getCellSize(), ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits());

    // Find the DN the first parity block is on and mark it dead on the NN.
    LocatedBlock parityBlk = blks[ecPolicy.getNumDataUnits()];
    int parityPort = parityBlk.getLocations()[0].getIpcPort();
    DataNode parityDN = cluster.getDataNode(parityPort);
    cluster.setDataNodeDead(parityDN.getDatanodeId());

    // Re-fetch the block locations - the node marked dead should no longer be included.
    blocks = client.getNamenode().getBlockLocations("/ecfiles/ecFile", 0, bytes);
    blockGroup = (LocatedStripedBlock) blocks.getLocatedBlocks().get(0);

    ECStripeReader ecStripeReader = new ECStripeReader(client, conf);
    try {
      ByteBuffer[] buffers = ecStripeReader.readStripe(blockGroup, ecPolicy, "/ecfiles/ecFile");
      fail("Expected Exception to be thrown");
    } catch (BlockUnavailableException e) {
      assertTrue(e.getMessage().matches("^Parity block in position.*is unavailable"));
    }
  }

  @Test
  public void testFileWithUnavailableDataBlockThrows() throws Exception {
    Path ecFile = new Path(ecRoot, "ecFile");
    // write one full stripe
    int bytes = ecPolicy.getNumDataUnits() * ecPolicy.getCellSize();
    createFileOfLength(ecFile, bytes);

    LocatedBlocks blocks = client.getNamenode().getBlockLocations("/ecfiles/ecFile", 0, bytes);
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) blocks.getLocatedBlocks().get(0);

    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(
        blockGroup, ecPolicy.getCellSize(), ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits());

    // Find the DN the first parity block is on and mark it dead on the NN.
    LocatedBlock parityBlk = blks[0];
    int dataPort = parityBlk.getLocations()[0].getIpcPort();
    DataNode parityDN = cluster.getDataNode(dataPort);
    cluster.setDataNodeDead(parityDN.getDatanodeId());

    // Re-fetch the block locations - the node marked dead should no longer be included.
    blocks = client.getNamenode().getBlockLocations("/ecfiles/ecFile", 0, bytes);
    blockGroup = (LocatedStripedBlock) blocks.getLocatedBlocks().get(0);

    ECStripeReader ecStripeReader = new ECStripeReader(client, conf);
    try {
      ByteBuffer[] buffers = ecStripeReader.readStripe(blockGroup, ecPolicy, "/ecfiles/ecFile");
      fail("Expected Exception to be thrown");
    } catch (BlockUnavailableException e) {
      assertTrue(e.getMessage().matches("^Data block in position.*is unavailable"));
    }
  }

  private void createFileOfLength(Path dest, int bytes) throws IOException {
    FSDataOutputStream stream = null;
    try {
      stream = fs.create(dest);
      for (int i = 0; i < bytes; i++) {
        stream.write(RandomUtils.nextBytes(1));
      }
    } finally {
      stream.close();
    }
  }
}
