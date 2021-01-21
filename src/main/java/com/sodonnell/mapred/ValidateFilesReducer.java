package com.sodonnell.mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValidateFilesReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text text, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    for (Text v : values) {
      context.write(text, v);
    }
  }

}
