package com.sodonnell.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ValidateFiles extends Configured implements Tool {

  private static Logger LOG = LoggerFactory.getLogger(ValidateFiles.class);

  private FileSystem fs = null;

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    fs = FileSystem.get(getConf());

    String stagingDir = args[0];
    String outputDir = args[1];
    int spilts = Integer.parseInt(args[2]);

    if (fs.exists(new Path(stagingDir))) {
      LOG.error("Staging directory already exists. Please remove it and rerun");
      return 1;
    }

    if (fs.exists(new Path(outputDir))) {
      LOG.error("Output directory already exists. Please remove it and rerun");
      return 1;
    }

    if (spilts < 1) {
      LOG.error("Spilts is less than 1. Please pass a value greater than 1");
      return 1;
    }

    List<String> pathsToProcess = new ArrayList<>();
    for (int i=3; i<args.length; i++) {
      Path toProcess = new Path(args[i]);
      if (!fs.exists(toProcess)) {
        LOG.warn("Input path {} does not exist", args[i]);
      } else {
        pathsToProcess.add(args[i]);
      }
    }

    LOG.info("Going to scan {} input paths for EC files", pathsToProcess.size());

    FileListing fileListing = new FileListing(fs, stagingDir, spilts, pathsToProcess);
    int fileCount = fileListing.generateListing();

    LOG.info("Found {} files to check", fileCount);

    // Create job
    Job job = Job.getInstance(conf, "EC-File-Validator");
    job.setJarByClass(getClass());

    // Setup MapReduce
    job.setMapperClass(ValidateFilesMapper.class);
    job.setInputFormatClass(TextInputFormat.class);

    job.setNumReduceTasks(1);
    job.setReducerClass(ValidateFilesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Input
    FileInputFormat.addInputPath(job, new Path(stagingDir));
    job.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    job.setOutputFormatClass(TextOutputFormat.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ValidateFiles(), args);
    System.exit(exitCode);
  }
}
