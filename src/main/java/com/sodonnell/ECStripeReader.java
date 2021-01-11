package com.sodonnell;

import com.sodonnell.exceptions.BlockUnavailableException;
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

public class ECStripeReader {

  private Logger LOG = LoggerFactory.getLogger(ECStripeReader.class);
  private Configuration conf;
  private DFSClient client;

  ECStripeReader(DFSClient client, Configuration conf) {
    this.client = client;
    this.conf = conf;
  }

  // TODO - accept buffer and reuse them.
  public ByteBuffer[] readStripe(LocatedStripedBlock block, ErasureCodingPolicy ecPolicy, String path)
      throws IOException {
    LOG.debug("The striped block size is given as {}", block.getBlockSize());
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(
        block, ecPolicy.getCellSize(), ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits());
    ByteBuffer[] buffers = ECValidateUtil.allocateBuffers(
        ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(), ecPolicy.getCellSize());

    ensureAllBlocksPresent(blks, ecPolicy, block);

    for (int i=0; i<ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(); i++) {
      BlockReader br = null;
      try {
        LocatedBlock blk = blks[i];
        if (blk == null) {
          LOG.debug("Block at index {} is null", i);
          continue;
        }
        LOG.debug("Block at index {} is of length {}", i, blk.getBlockSize());
        long bytesToRead = blk.getBlockSize() < ecPolicy.getCellSize() ? blk.getBlockSize() : ecPolicy.getCellSize();
        br = getBlockReader(client, path, conf, blks[i], 0, bytesToRead);
        while(buffers[i].hasRemaining()) {
          int read = br.read(buffers[i]);
          if (read < 0) {
            break;
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to read block {}", blks[i], e);
        throw e;
      } finally {
        if (br != null) {
          br.close();
        }
      }
    }
    return buffers;
  }

  /**
   * We can only validate the EC integrity if all the replicas are online. Therefore
   * we need to check for null blocks in the the LocatedBlocks. However if the block
   * is less than the stripe width, then we expect to have some null blocks.
   * Parity blocks should never be null.
   * @param blks
   * @param ecPolicy
   * @param blkGroup
   */
  private void ensureAllBlocksPresent(LocatedBlock[] blks, ErasureCodingPolicy ecPolicy, LocatedStripedBlock blkGroup)
      throws BlockUnavailableException, UnExpectedBlockException {
    // All parity blocks should always be present
    for (int i=ecPolicy.getNumDataUnits(); i<ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(); i++) {
      if (blks[i] == null) {
        throw new BlockUnavailableException("Parity block in position " + i + " of block " + blkGroup.getBlock() + " is unavailable");
      }
      LOG.info("Parity {} is not null", i);
    }

    // For data blocks, we should have enough to make up the full stripe size.
    // Eg if block group is 1.5MB, then we expect 2 blocks (stripe size of 1MB)
    int expectedDataBlocks = (int)Math.ceil((double)blkGroup.getBlockSize() / ecPolicy.getCellSize());
    if (expectedDataBlocks > ecPolicy.getNumDataUnits()) {
      expectedDataBlocks = ecPolicy.getNumDataUnits();
    }
    for (int i=0; i<expectedDataBlocks; i++) {
      if (blks[i] == null) {
        throw new BlockUnavailableException("Data block in position " + i + " of block " + blkGroup.getBlock() + " is unavailable");
      }
    }
    for (int i=expectedDataBlocks; i<ecPolicy.getNumDataUnits(); i++) {
      if (blks[i] != null) {
        throw new UnExpectedBlockException("Data block in position " + i + " of block " + blkGroup.getBlock() + " is present, but should not be");
      }
    }
  }

  private BlockReader getBlockReader(DFSClient dfsClient, String src, Configuration conf, LocatedBlock targetBlock,
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
        setFileName(src).
        setBlock(blk).
        setBlockToken(accessToken).
        setStartOffset(offsetInBlock).
        setVerifyChecksum(verifyChecksum).
        setClientName(dfsClient.getClientName()).
        setLength(length).
        setAllowShortCircuitLocalReads(false).
        setClientCacheContext(dfsClient.getClientContext()).
        setUserGroupInformation(UserGroupInformation.getCurrentUser()). //dfsClient.ugi).
        setConfiguration(conf). //dfsClient.getConfiguration()).
        setCachingStrategy(CachingStrategy.newDefaultStrategy()).
        build();
  }

}
