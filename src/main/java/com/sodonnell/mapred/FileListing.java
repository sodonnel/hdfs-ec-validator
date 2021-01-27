package com.sodonnell.mapred;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

public class FileListing {

  private FileSystem fs;
  private List<String> srcPaths;
  private String outputPath;
  private int splits;
  private int fileCount = 0;

  private FSDataOutputStream[] outputHandles;

  public FileListing(FileSystem fs, String outputPath, int splits, List<String> srcPaths) {
    this.srcPaths = srcPaths;
    this.outputPath = outputPath;
    this.splits = splits;
    this.fs = fs;
    outputHandles = new FSDataOutputStream[this.splits];
  }

  public int generateListing(String inputPath) throws IOException {
    createOutputs();
    if (inputPath != null) {
      addInputPath(inputPath);
    }
    for (String s : srcPaths) {
      Path sourcePath = new Path(s);
      generateSubListing(sourcePath);
    }
    closeOutputs();
    return fileCount;
  }

  private void generateSubListing(Path root) throws IOException {
    FileStatus[] children = fs.listStatus(root);
    for (FileStatus f : children) {
      if (f.isDirectory()) {
        generateSubListing(new Path(root, f.getPath().getName()));
      } else {
        String path = root.toUri().getPath()+"/"+f.getPath().getName();
        writePathToOutput(path);
      }
    }
  }

  private void addInputPath(String inputPath) throws IOException {
    try (Stream<String> stream = Files.lines(Paths.get(inputPath))) {
      stream.forEach(l -> {
        try {
          writePathToOutput(l);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private void writePathToOutput(String path) throws IOException {
    outputHandles[(fileCount++)%splits].writeBytes(path + System.lineSeparator());
  }

  private void createOutputs() throws IOException {
    for (int i=0; i<splits; i++) {
      Path path = new Path(outputPath, "split_"+ StringUtils.leftPad(Integer.toString(i), 4, "0"));
      outputHandles[i] = fs.create(path);
    }
  }

  private void closeOutputs() throws IOException {
    for (FSDataOutputStream s : outputHandles) {
      s.close();
    }
  }

}
