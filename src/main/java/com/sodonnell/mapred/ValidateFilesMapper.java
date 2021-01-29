package com.sodonnell.mapred;

import com.sodonnell.ECFileValidator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

public class ValidateFilesMapper extends Mapper<LongWritable, Text, Text, BlockReport> {

  static final String FILE_FAIL_BLOCK = "fileFailed";

  ECFileValidator ecFileValidator = null;

  @Override
  public void setup(Context context) {
    try {
      ecFileValidator = new ECFileValidator(context.getConfiguration());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void cleanup(Context context) throws InterruptedException {
    try {
      if (ecFileValidator != null) {
        ecFileValidator.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String path = null;
    try {
      path = value.toString();
      Iterator<BlockReport> itr = ecFileValidator.validateBlocks(path, true);
      while (itr.hasNext()) {
        context.write(value, itr.next());
      }
    } catch (Exception e) {
      context.write(value, new BlockReport()
          .setBlockGroup(FILE_FAIL_BLOCK)
          .setFailed(true)
          .setMessage(e.getClass().toString()+ " " + e.getMessage()));
    }
  }

}
