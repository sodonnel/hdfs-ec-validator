package com.sodonnell.cli;

import com.sodonnell.ECFileValidator;
import com.sodonnell.ECValidatorConfigKeys;
import com.sodonnell.ValidationReport;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class BatchFile {

  private static Logger LOG = LoggerFactory.getLogger(BatchFile.class);

  public static void main(String[] args) throws Exception {
    // Expect one or two arguments:
    //   First an input file, with a file to check on each line
    //   Second is an output file to write the results to. If it is not specified
    //   then the results are written to stdout.
    if (args.length < 1 || args.length > 2) {
      System.out.println("Usage java com.sodonnell.cli.BatchFile inputFile <outputFile>");
      System.exit(1);
    }
    File input = new File(args[0]);
    File outputFile;
    if (!input.exists()) {
      System.out.println("Input file " + input.getPath() + " does not exist");
      System.exit(1);
    }
    FileInputStream fstream = new FileInputStream(input);
    BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

    PrintStream out;
    if (args.length == 2) {
      outputFile = new File(args[1]);
      out = new PrintStream(outputFile);
    } else {
      out = System.out;
    }

    Configuration conf = new Configuration();

    String fs = conf.get(ECValidatorConfigKeys.ECVALIDATOR_FIELD_SEPARATOR_KEY,
        ECValidatorConfigKeys.ECVALIDATOR_FIELD_SEPARATOR_DEFAULT);

    try (ECFileValidator validator = new ECFileValidator(conf)) {
      String l;
      while ((l = br.readLine()) != null) {
        // Print the content on the console
        try {
          ValidationReport res = validator.validate(l, true);
          String zeroParity = "" ;
          if (res.isParityAllZero()) {
            zeroParity = "zeroParityBlockGroups " + StringUtils.join(res.parityAllZeroBlockGroups(), ",");
          }
          if (res.isHealthy()) {
            out.println("healthy" + fs + l + fs + zeroParity);
          } else {
            String msg = StringUtils.join(res.corruptBlockGroups(), ",");
            if (zeroParity != "") {
              msg = msg + " " + zeroParity;
            }
            out.println("corrupt" + fs + l + fs + msg);
          }
        } catch (Exception e) {
          LOG.debug("Failed to read file {}", l, e);
          out.println("failed" + fs + l + fs + e.getClass().getSimpleName() + ":" + e.getMessage());
        }
      }
    }
    br.close();
  }
}
