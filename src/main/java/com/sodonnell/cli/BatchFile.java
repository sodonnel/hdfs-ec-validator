package com.sodonnell.cli;

import com.sodonnell.ECFileValidator;
import com.sodonnell.ValidationReport;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class BatchFile {

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

    try (ECFileValidator validator = new ECFileValidator(conf)) {
      String l;
      while ((l = br.readLine()) != null) {
        // Print the content on the console
        try {
          ValidationReport res = validator.validate(l, true);
          String zeroParity = "" ;
          if (res.isParityAllZero()) {
            zeroParity = " zeroParityBlockGroups " + StringUtils.join(res.parityAllZeroBlockGroups(), ",");
          }
          if (res.isHealthy()) {
            out.println("healthy " + l + zeroParity);
          } else {
            out.println("corrupt " + l + " " + StringUtils.join(res.corruptBlockGroups(), ",") + zeroParity);
          }
        } catch (Exception e) {
          out.println("failed " + l + " " + e.getClass().toString() + ":" + e.getMessage());
        }
      }
    }
    br.close();
  }
}
