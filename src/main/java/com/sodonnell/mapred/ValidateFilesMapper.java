package com.sodonnell.mapred;

import com.sodonnell.ECFileValidator;
import com.sodonnell.ValidationReport;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ValidateFilesMapper extends Mapper<LongWritable, Text, Text, Text> {

  ECFileValidator ecFileValidator = null;
  Text message = new Text();
  Text healthy = new Text("healthy");
  Text corrupt = new Text("corrupt");
  Text failed = new Text("failed");


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
      ValidationReport res = ecFileValidator.validate(path, true);
      if (res.isHealthy()) {
        message.set(path);
        context.write(healthy, message);
      } else {
        message.set(path + " " + StringUtils.join(res.corruptBlockGroups(), ","));
        context.write(corrupt, message);
      }
    } catch (Exception e) {
      message.set(path + " " + e.getMessage());
      context.write(failed, message);
    }
  }

}
