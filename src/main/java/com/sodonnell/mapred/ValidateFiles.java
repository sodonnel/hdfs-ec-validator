package com.sodonnell.mapred;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

  private static String OPT_OUTPUTDIR = "outputdir";
  private static String OPT_STAGEDIR = "stagedir";
  private static String OPT_MAPPERS = "mappers";
  private static String OPT_INPUTFILE = "inputfile";
  private static String OPT_INPUTDIR= "inputdir";


  private FileSystem fs = null;
  private Options options = new Options();

  private String stagingDir;
  private String outputDir;
  private int splits;
  private String inputFile = null;

  private CommandLine setupOptions(String[] args) throws ParseException {
    Option opt;
    opt = new Option("s", OPT_STAGEDIR, true, "Staging directory to create split files");
    opt.setRequired(true);
    options.addOption(opt);

    opt = new Option("o", OPT_OUTPUTDIR, true, "Directory to write the results in");
    opt.setRequired(true);
    options.addOption(opt);

    opt = new Option("m", OPT_MAPPERS, true, "Number of mappers to use in the job");
    opt.setRequired(true);
    options.addOption(opt);

    options.addOption("f", OPT_INPUTFILE, true, "Input file containing all paths to check");
    options.addOption("i", OPT_INPUTDIR, true, "An input directory of files to process recursively");

    return new GnuParser().parse(options, args);
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLine opts;
    try {
      opts = setupOptions(args);
    } catch(ParseException e) {
      System.out.println("Unexpected exception:" + e.getMessage());
      new HelpFormatter().printHelp("com.sodonnell.mapred.ValidateFiles", options);
      return 1;
    }
    Configuration conf = getConf();
    fs = FileSystem.get(getConf());

    stagingDir = opts.getOptionValue(OPT_STAGEDIR);
    outputDir = opts.getOptionValue(OPT_OUTPUTDIR);
    splits = Integer.parseInt(opts.getOptionValue(OPT_MAPPERS));

    if (fs.exists(new Path(stagingDir))) {
      LOG.error("Staging directory already exists. Please remove it and rerun");
      return 1;
    }

    if (fs.exists(new Path(outputDir))) {
      LOG.error("Output directory already exists. Please remove it and rerun");
      return 1;
    }

    if (splits < 1) {
      LOG.error("Spilts is less than 1. Please pass a value greater than 1");
      return 1;
    }

    List<String> pathsToProcess = new ArrayList<>();
    if (opts.hasOption(OPT_INPUTDIR)) {
      String[] inputPaths = opts.getOptionValues(OPT_INPUTDIR);
      for (String path : inputPaths) {
        Path toProcess = new Path(path);
        if (!fs.exists(toProcess)) {
          LOG.warn("Input path {} does not exist", path);
        } else {
          pathsToProcess.add(path);
        }
      }
    }
    LOG.info("Going to scan {} input paths for EC files", pathsToProcess.size());

    if (options.hasOption(OPT_INPUTFILE)) {
      inputFile = opts.getOptionValue(OPT_INPUTFILE);
      LOG.info("Going to scan EC files in {}", inputFile);
    }

    FileListing fileListing = new FileListing(fs, stagingDir, splits, pathsToProcess);
    int fileCount = fileListing.generateListing(inputFile);

    if (fileCount == 0) {
      LOG.error("There are no input files to scan");
      return 1;
    } else {
      LOG.info("Found {} files to check", fileCount);
    }

    // Create job
    Job job = Job.getInstance(conf, "EC-File-Validator");
    job.setJarByClass(getClass());

    // Setup MapReduce
    job.setMapperClass(ValidateFilesMapper.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BlockReport.class);

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
