package com.sodonnell;

import com.sodonnell.exceptions.NotErasureCodedException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ECFileValidator implements Closeable {

  Logger LOG = LoggerFactory.getLogger(ECFileValidator.class);
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

  public ValidationReport validate(String src, boolean checkOnlyFirstStripe) throws Exception {
    Path file = new Path(src);
    if (!fs.exists(file)) {
      throw new FileNotFoundException("File "+src+" does not exist");
    }
    FileStatus stat = fs.getFileStatus(file);
    if (!stat.isErasureCoded()) {
      throw new NotErasureCodedException("File "+src+" is not erasure coded");
    }

    LOG.info("Going to validate {}", src);
    ValidationReport report = new ValidationReport();
    LocatedBlocks fileBlocks = client.getNamenode().getBlockLocations(src, 0, stat.getLen());
    ErasureCodingPolicy ecPolicy = fileBlocks.getErasureCodingPolicy();

    ByteBuffer[] stripe = null;
    for (LocatedBlock b : fileBlocks.getLocatedBlocks()) {
      LOG.info("checking block {} of size {}", b.getBlock(), b.getBlockSize());
      LocatedStripedBlock sb = (LocatedStripedBlock)b;
      try (StripedBlockReader br = new StripedBlockReader(client, conf, sb, ecPolicy, executor)) {
        int stripeNum = 0;
        boolean corrupt = false;
        boolean zeroParity = true;
        while(true) {
          if (stripe == null) {
            stripe = ECValidateUtil.allocateBuffers(
                ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(), ecPolicy.getCellSize());
          } else {
            ECValidateUtil.clearBuffers(stripe);
          }
          if (br.readNextStripe(stripe) == 0) {
            break;
          }
          stripeNum ++;
          if (ECChecker.allZeroParity(stripe, ecPolicy) == 0) {
            zeroParity = false;
          }
          if (!ECChecker.validateParity(stripe, ecPolicy)) {
            corrupt = true;
            break;
          }
          if (checkOnlyFirstStripe) {
            break;
          }
        }
        if (zeroParity) {
          report.addZeroParityBlockGroup(b.getBlock().getLocalBlock().toString(), stripeNum);
        }
        if (corrupt) {
          report.addCorruptBlockGroup(b.getBlock().getLocalBlock().toString(), stripeNum);
        } else {
          report.addValidBlockGroup(b.getBlock().getLocalBlock().toString(), stripeNum);
        }
      }
    }
    return report;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    try (ECFileValidator validator = new ECFileValidator(conf)) {
      for (String f : args) {
        try {
          ValidationReport res = validator.validate(f, true);
          String zeroParity = "" ;
          if (res.isParityAllZero()) {
            zeroParity = " zeroParityBlockGroups " + StringUtils.join(res.parityAllZeroBlockGroups(), ",");
          }
          if (res.isHealthy()) {
            System.out.println("healthy " + f + zeroParity);
          } else {
            System.out.println("corrupt " + f + " " + StringUtils.join(res.corruptBlockGroups(), ",") + zeroParity);
          }
        } catch (Exception e) {
          System.out.println("failed " + f + " " + e.getClass().toString() + ":" + e.getMessage());
        }
      }
    }
  }

}
