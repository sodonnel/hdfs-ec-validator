package com.sodonnell;

import com.sodonnell.exceptions.NotErasureCodedException;
import com.sodonnell.mapred.BlockReport;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ECFileValidator implements Closeable {

  private static Logger LOG = LoggerFactory.getLogger(ECFileValidator.class);
  private Configuration conf;
  private DFSClient client;
  private FileSystem fs;
  private ExecutorService executor;

  public ECFileValidator(Configuration conf) throws Exception {
    this.conf = conf;
    fs = FileSystem.get(conf);
    client = new DFSClient(fs.getUri(), conf);
    createExecutor();
  }

  @Override
  public void close() throws IOException {
    executor.shutdown();
  }

  private void createExecutor() throws IOException {
    int threads = 0;
    for (ErasureCodingPolicyInfo ecp : client.getErasureCodingPolicies()) {
      LOG.info("EC Policy {} exists", ecp.getPolicy().getName());
      int policyThreads = ecp.getPolicy().getNumDataUnits() + ecp.getPolicy().getNumParityUnits();
      threads = Math.max(threads, policyThreads);
    }
    LOG.info("Created reader thread pool with {} threads", threads);
    executor = Executors.newFixedThreadPool(threads);
  }

  public Iterator<BlockReport> validateBlocks(String src, boolean checkOnlyFirstStripe) throws Exception {
    Path file = new Path(src);
    if (!fs.exists(file)) {
      throw new FileNotFoundException("File "+src+" does not exist");
    }
    FileStatus stat = fs.getFileStatus(file);
    if (!stat.isErasureCoded()) {
      throw new NotErasureCodedException("File "+src+" is not erasure coded");
    }

    LocatedBlocks fileBlocks = client.getNamenode().getBlockLocations(src, 0, stat.getLen());
    ErasureCodingPolicy ecPolicy = fileBlocks.getErasureCodingPolicy();

    LOG.info("Going to validate {} with {} blocks", src, fileBlocks.getLocatedBlocks().size());
    ByteBuffer[] stripe = ECValidateUtil.allocateBuffers(
        ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(), ecPolicy.getCellSize());

    final Iterator<LocatedBlock> blockItr = fileBlocks.getLocatedBlocks().iterator();

    return new Iterator<BlockReport>() {
      @Override
      public boolean hasNext()
      {
        return blockItr.hasNext();
      }

      @Override
      public BlockReport next()
      {
        if (hasNext())
        {
          LocatedStripedBlock sb = (LocatedStripedBlock)blockItr.next();
          try {
            return processBlock(sb, ecPolicy, checkOnlyFirstStripe, stripe);
          } catch (Exception e) {
            LOG.warn("Failed processing {}", sb, e);
            return new BlockReport()
                .setBlockGroup(sb.getBlock().getLocalBlock().toString())
                .setFailed(true)
                .setMessage(e.getClass().getSimpleName() + " " + e.getMessage());
          }
        }
        throw new NoSuchElementException("No more blocks available");
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("Removals are not supported");
      }};
  }

  public ValidationReport validate(String src, boolean checkOnlyFirstStripe) throws Exception {
    Iterator<BlockReport> validateBlocks = validateBlocks(src, checkOnlyFirstStripe);
    ValidationReport report = new ValidationReport();

    while (validateBlocks.hasNext()) {
      BlockReport blockReport = validateBlocks.next();
      if (blockReport.hasZeroParity()) {
        report.addZeroParityBlockGroup(blockReport.blockGroup(), blockReport.stripesChecked(), blockReport.message());
      }
      if (blockReport.isCorrupt()) {
        report.addCorruptBlockGroup(blockReport.blockGroup(), blockReport.stripesChecked());
      } else {
        report.addValidBlockGroup(blockReport.blockGroup(), blockReport.stripesChecked());
      }
    }
    return report;
  }

  private BlockReport processBlock(LocatedStripedBlock sb, ErasureCodingPolicy ecPolicy, boolean checkOnlyFirstStripe,
      ByteBuffer[] stripe) throws Exception {
    BlockReport report = new BlockReport();
    report.setBlockGroup(sb.getBlock().getLocalBlock().toString());

    try (StripedBlockReader br = new StripedBlockReader(client, conf, sb, ecPolicy, executor)) {
      int stripeNum = 0;
      Set<Integer> nonZeroParityIndicies = new HashSet<>();
      while(true) {
        ECValidateUtil.clearBuffers(stripe);
        if (br.readNextStripe(stripe) == 0) {
          break;
        }
        stripeNum ++;
        if (nonZeroParityIndicies.size() < ecPolicy.getNumParityUnits()) {
          nonZeroParityIndicies.addAll(ECChecker.getNonZeroParityIndicies(stripe, ecPolicy));
        }
        if (!ECChecker.validateParity(stripe, ecPolicy)) {
          report.setIsCorrupt(true);
          break;
        }
        if (checkOnlyFirstStripe) {
          break;
        }
      }
      if (nonZeroParityIndicies.size() < ecPolicy.getNumParityUnits()) {
        String details = getZeroParityBlocks(br, ecPolicy, nonZeroParityIndicies);
        report.setHasZeroParity(true);
        report.setMessage(details);
      }
      report.setStripesChecked(stripeNum);
    }
    return report;
  }

  private String getZeroParityBlocks(StripedBlockReader sb, ErasureCodingPolicy ecPolicy, Set<Integer> nonZeroIndicies) {
    StringBuilder bldr = new StringBuilder();
    boolean first = true;
    for (int i=ecPolicy.getNumDataUnits(); i<ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(); i++) {
      if (!nonZeroIndicies.contains(i)) {
        if (first) {
          first = false;
          bldr.append("[");
        } else {
          bldr.append("|");
        }
        LocatedBlock block = sb.getBlockAtIndex(i);
        block.getBlock().getLocalBlock().appendStringTo(bldr);
      }
    }
    bldr.append("]");
    return bldr.toString();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String fs = conf.get(ECValidatorConfigKeys.ECVALIDATOR_FIELD_SEPARATOR_KEY,
        ECValidatorConfigKeys.ECVALIDATOR_FIELD_SEPARATOR_DEFAULT);

    try (ECFileValidator validator = new ECFileValidator(conf)) {
      for (String f : args) {
        try {
          ValidationReport res = validator.validate(f, true);
          String zeroParity = "" ;
          if (res.isParityAllZero()) {
            zeroParity = "zeroParityBlockGroups " + StringUtils.join(res.parityAllZeroBlockGroups(), ",");
          }
          if (res.isHealthy()) {
            System.out.println("healthy" + fs + f + fs + zeroParity);
          } else {
            String msg = StringUtils.join(res.corruptBlockGroups(), ",");
            if (zeroParity != "") {
              msg = msg + " " + zeroParity;
            }
            System.out.println("corrupt" + fs + f + fs + msg);
          }
        } catch (Exception e) {
          LOG.debug("Failed to read file {}", f, e);
          System.out.println("failed" + fs + f + fs + e.getClass().getSimpleName() + ":" + e.getMessage());
        }
      }
    }
  }

}
