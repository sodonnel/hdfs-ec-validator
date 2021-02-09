package com.sodonnell;

import com.sodonnell.exceptions.BlockUnavailableException;
import com.sodonnell.exceptions.MisalignedBuffersException;
import com.sodonnell.exceptions.UnExpectedBlockException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Read data from a striped block into byte buffers.
 */

public class StripedBlockReader implements AutoCloseable {

  private final static Logger LOG = LoggerFactory.getLogger(StripedBlockReader.class);

  private final DFSClient dfsClient;
  private final Configuration conf;
  private final LocatedStripedBlock block;
  private final LocatedBlock[] locatedBlocks;
  private final ErasureCodingPolicy ecPolicy;
  private final ExecutorService executor;
  private final BlockReader[] blockReaders;
  private final int readTimout;

  public StripedBlockReader(DFSClient dfsClient, Configuration conf, LocatedStripedBlock block,
      ErasureCodingPolicy ecPolicy, ExecutorService executor) throws IOException {
    this.dfsClient = dfsClient;
    this.conf = conf;
    this.block = block;
    this.ecPolicy = ecPolicy;
    this.executor = executor;

    this.locatedBlocks = StripedBlockUtil.parseStripedBlockGroup(
        block, ecPolicy.getCellSize(), ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits());
    ensureAllBlocksPresent();
    blockReaders = new BlockReader[ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()];

    this.readTimout = conf.getInt(ECValidatorConfigKeys.ECVALIDATOR_READ_TIMEOUT,
        ECValidatorConfigKeys.ECVALIDATOR_READ_TIMEOUT_DEFAULT);
  }

  public LocatedBlock getBlockAtIndex(int i) {
    return locatedBlocks[i];
  }

  public void close() {
    for (BlockReader r : blockReaders) {
      try {
        if (r != null) {
          r.close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close BlockReader {}", r, e);
      }
    }
  }

  /**
   * Reads a stripe of data for the block into given buffer array. The buffers
   * should have the ECPolicy.cellSize() of available space to read the data into.
   * Data is read from each block until it fills the buffers.
   * Data will be read into the buffer until the buffer is full, or there is
   * no further data in the block.
   * This routine will return the total bytes read if any data was successfully read,
   * or zero indicating EOF.
   * After EOF is reached, further calls with continue to return zero.
   * This means that if a previous read consumed all available data, you must
   * make a further call to reach EOF.
   * @param buffers
   * @return 0 if EOF or the total bytes read from all blocks if any data was read
   */
  public int readNextStripe(ByteBuffer[] buffers) throws Exception {
    validateBuffers(buffers);
    Queue<Future<Integer>> pendingReads = new ArrayDeque();
    for (int i=0; i<blockReaders.length; i++) {
      final int ind = i;
      if (locatedBlocks[ind] == null) {
        continue;
      }
      final ByteBuffer buf = buffers[i];
      final LocatedBlock blk = locatedBlocks[i];

      pendingReads.add(executor.submit(() -> {
        if (blockReaders[ind] == null) {
          blockReaders[ind] = getBlockReader(dfsClient, conf, blk, 0, blk.getBlockSize());
        }
        int totalRead = 0;
        while(buf.hasRemaining()) {
          int read = blockReaders[ind].read(buf);
          if (read < 0) {
            if (totalRead > 0) {
              return totalRead;
            } else {
              return -1;
            }
          } else {
            totalRead += read;
          }
        }
        return totalRead;
      }));
    }

    int totalRead = 0;
    while(!pendingReads.isEmpty()) {
      Future<Integer> f = pendingReads.poll();
      try {
        // If any future returns zero, then the return value will be zero.
        // Only when all futures return -1, should we return -1.
        // TODO - should we timeout here, or rely on the underlying HDFS Client timeouts?
        int read = f.get(readTimout, TimeUnit.SECONDS);
        if (read > 0) {
          totalRead += read;
        }
      } catch (TimeoutException e) {
        f.cancel(true);
        LOG.error("Timeout attempting to read block. Pending read count {}", pendingReads.size());
        throw e;
      } catch (InterruptedException e) {
        throw e;
      } catch (ExecutionException e) {
        throw e;
      }
    }
    return totalRead;
  }

  private void validateBuffers(ByteBuffer[] buffers) throws IOException {
    if (buffers.length != ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()) {
      throw new MisalignedBuffersException("Insufficient buffers (" + buffers.length +") for EC Policy " + ecPolicy.getName());
    }
    for (ByteBuffer b : buffers) {
      if (b == null) {
        throw new MisalignedBuffersException("A buffer is null");
      }
      if (b.remaining() != ecPolicy.getCellSize()) {
        throw new MisalignedBuffersException("A buffer has less space than the EC Cell size of " + ecPolicy.getCellSize());
      }
    }
  }

  /**
   * We can only validate the EC integrity if all the replicas are online. Therefore
   * we need to check for null blocks in the the LocatedBlocks. However if the block
   * is less than the stripe width, then we expect to have some null blocks.
   * Parity blocks should never be null.
   */
  private void ensureAllBlocksPresent()
      throws BlockUnavailableException, UnExpectedBlockException {
    // All parity blocks should always be present
    for (int i=ecPolicy.getNumDataUnits(); i<ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(); i++) {
      if (locatedBlocks[i] == null) {
        throw new BlockUnavailableException("Parity block in position " + i + " of block " + block.getBlock() + " is unavailable");
      }
    }

    // For data blocks, we should have enough to make up the full stripe size.
    // Eg if block group is 1.5MB, then we expect 2 blocks (stripe size of 1MB)
    int expectedDataBlocks = (int)Math.ceil((double)block.getBlockSize() / ecPolicy.getCellSize());
    if (expectedDataBlocks > ecPolicy.getNumDataUnits()) {
      expectedDataBlocks = ecPolicy.getNumDataUnits();
    }
    for (int i=0; i<expectedDataBlocks; i++) {
      if (locatedBlocks[i] == null) {
        throw new BlockUnavailableException("Data block in position " + i + " of block " + block.getBlock() + " is unavailable");
      }
    }
    for (int i=expectedDataBlocks; i<ecPolicy.getNumDataUnits(); i++) {
      if (locatedBlocks[i] != null) {
        throw new UnExpectedBlockException("Data block in position " + i + " of block " + block.getBlock() + " of length "
            + block.getBlockSize() + " is present, but should not be");
      }
    }
  }

  private BlockReader getBlockReader(DFSClient dfsClient, Configuration conf, LocatedBlock targetBlock,
                                     long offsetInBlock, long length) throws IOException {

    StorageType[] storageTypes = targetBlock.getStorageTypes();
    DatanodeInfo[] datanodeInfos = targetBlock.getLocations();

    // For now, just use the first datanode we find.
    DatanodeInfo datanode = datanodeInfos[0];

    ExtendedBlock blk = targetBlock.getBlock();
    Token<BlockTokenIdentifier> accessToken = targetBlock.getBlockToken();

    final String dnAddr =
        datanode.getXferAddr(dfsClient.getConf().isConnectToDnViaHostname());
    InetSocketAddress targetAddr = NetUtils.createSocketAddr(dnAddr,-1, null);

    boolean verifyChecksum = conf.getBoolean(ECValidatorConfigKeys.ECVALIDATOR_VERIFY_CHECKSUMS,
        ECValidatorConfigKeys.ECVALIDATOR_VERIFY_CHECKSUMS_DEFAULT);

    return new BlockReaderFactory(dfsClient.getConf()).
        setInetSocketAddress(targetAddr).
        setRemotePeerFactory(dfsClient).
        setDatanodeInfo(datanode).
        setStorageType(storageTypes[0]).
        setBlock(blk).
        setBlockToken(accessToken).
        setStartOffset(offsetInBlock).
        setVerifyChecksum(verifyChecksum).
        setClientName(dfsClient.getClientName()).
        setLength(length).
        setAllowShortCircuitLocalReads(false).
        setClientCacheContext(dfsClient.getClientContext()).
        setUserGroupInformation(UserGroupInformation.getCurrentUser()).
        setConfiguration(conf).
        setCachingStrategy(CachingStrategy.newDefaultStrategy()).
        build();
  }

}
