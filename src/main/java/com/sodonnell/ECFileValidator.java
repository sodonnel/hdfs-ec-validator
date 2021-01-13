package com.sodonnell;

import com.sodonnell.exceptions.NotErasureCodedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ECFileValidator {

  Logger LOG = LoggerFactory.getLogger(ECFileValidator.class);
  private Configuration conf;
  private DFSClient client;
  private FileSystem fs;
  private ECStripeReader ecStripeReader;

  public ECFileValidator(Configuration conf) throws Exception {
    this.conf = conf;
    fs = FileSystem.get(conf);
    client = new DFSClient(fs.getUri(), conf);
    ecStripeReader = new ECStripeReader(client, conf);
  }

  public ValidationReport validate(String src) throws IOException {
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

    for (LocatedBlock b : fileBlocks.getLocatedBlocks()) {
      LOG.info("checking block {} of size {}", b.getBlock(), b.getBlockSize());
      LocatedStripedBlock sb = (LocatedStripedBlock)b;
      ByteBuffer[] stripe = ecStripeReader.readStripe(sb, ecPolicy, src);
      boolean res = ECChecker.validateParity(stripe, ecPolicy);
      if (res == false) {
        report.addCorruptBlockGroup(b.getBlock().getLocalBlock().toString());
      } else {
        report.addValidBlockGroup(b.getBlock().getLocalBlock().toString());
      }
    }
    return report;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    ECFileValidator validator = new ECFileValidator(conf);

    for (String f : args) {
      try {
        ValidationReport res = validator.validate(f);
        if (res.isHealthy()) {
          System.out.println("healthy " + f);
        } else {
          System.out.println("corrupt " + f + " " + StringUtils.join(res.corruptBlockGroups(), ","));
        }
      } catch (Exception e) {
        System.out.println("failed " + f + " " + e.getMessage());
      }
    }
  }

}
