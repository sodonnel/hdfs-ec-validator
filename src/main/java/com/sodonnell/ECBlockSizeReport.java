package com.sodonnell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ECBlockSizeReport implements AutoCloseable {

  private static Logger LOG = LoggerFactory.getLogger(ECBlockSizeReport.class);

  private static long THRESHOLD = Integer.MAX_VALUE;

  private Configuration conf;
  private DFSClient client;
  private FileSystem fs;

  public ECBlockSizeReport(Configuration conf) throws IOException {
    this.conf = conf;
    fs = FileSystem.get(conf);
    client = new DFSClient(fs.getUri(), conf);
  }

  public void report(String path) throws IOException {
    Path p = new Path(path);
    generateSubListing(p);;
  }

  @Override
  public void close() {
  }

  private void generateSubListing(Path root) throws IOException {
    FileStatus[] children = fs.listStatus(root);
    for (FileStatus f : children) {
      if (f.isDirectory()) {
        generateSubListing(new Path(root, f.getPath().getName()));
      } else {
        String subPath = root.toUri().getPath()+"/"+f.getPath().getName();
        reportPath(new Path(subPath));
      }
    }
  }

  public void reportPath(Path path) throws IOException {
    FileStatus stat = fs.getFileStatus(path);
    if (!stat.isErasureCoded()) {
      return;
    }
    LocatedBlocks fileBlocks = client.getNamenode().getBlockLocations(path.toUri().getPath(), 0, stat.getLen());
    ErasureCodingPolicy ecPolicy = fileBlocks.getErasureCodingPolicy();
    validatePath(path, fileBlocks, ecPolicy);
  }

  public void validatePath(Path path, LocatedBlocks blocks, ErasureCodingPolicy ecPolicy) {
    if (blocks.locatedBlockCount() <= 1) {
      output("undetermined," + path.toUri().toString() + ",fewer than 2 blocks");
    }
    long blockSize = blocks.get(0).getBlockSize();
    if (blockSize > THRESHOLD) {
      output("impacted," +path.toUri().toString() + ",ECPolicy: " + ecPolicy.getName() + " BlockSize: "
          + blockSize / ecPolicy.getNumDataUnits() + " BlockGroupSize: " + blockSize);
    }
  }

  private void output(String s) {
    System.out.println(s);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    try (ECBlockSizeReport report = new ECBlockSizeReport(conf)) {
      for (String f : args) {
        try {
          report.report(f);
        } catch (Exception e) {
        }
      }
    }
  }

}
