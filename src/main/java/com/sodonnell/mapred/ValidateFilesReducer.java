package com.sodonnell.mapred;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ValidateFilesReducer extends Reducer<Text, BlockReport, Text, Text> {

  private Text failed = new Text("failed");
  private Text corrupt = new Text("corrupt");
  private Text healthy = new Text("healthy");

  private Text message = new Text();

  public void reduce(Text text, Iterable<BlockReport> values, Context context)
      throws IOException, InterruptedException {

    List<String> corruptBlocks = new ArrayList<>();
    List<String> zeroParity = new ArrayList<>();
    List<String> failedBlocks = new ArrayList<>();
    boolean fileFailure = false;
    String firstFailureReason = null;
    for (BlockReport r : values) {
      if (r.failed()) {
        if (firstFailureReason == null) {
          firstFailureReason = r.message();
        }
        if (r.blockGroup().equals(ValidateFilesMapper.FILE_FAIL_BLOCK)) {
          fileFailure = true;
          break;
        }
        failedBlocks.add(r.blockGroup());
      }
      if (r.isCorrupt()) {
        corruptBlocks.add(r.blockGroup());
      }
      if (r.hasZeroParity()) {
        zeroParity.add(r.blockGroup());
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append(text.toString());
    if (firstFailureReason != null) {
      sb.append(" " + firstFailureReason);
    }

    if (fileFailure) {
      context.write(failed, new Text(sb.toString()));
      return;
    }

    if (corruptBlocks.size() > 0) {
      sb.append(" corrupt block groups {");
      sb.append(StringUtils.join(corruptBlocks, ","));
      sb.append("}");
    }
    if (zeroParity.size() > 0) {
      sb.append(" zero parity block groups {");
      sb.append(StringUtils.join(zeroParity, ","));
      sb.append("}");
    }
    if (failedBlocks.size() > 0) {
      sb.append(" failed block groups {");
      sb.append(StringUtils.join(failedBlocks, ","));
      sb.append("}");
    }

    if (failedBlocks.size() > 0) {
      context.write(failed, new Text(sb.toString()));
    } else if (corruptBlocks.size() > 0) {
      context.write(corrupt, new Text(sb.toString()));
    } else {
      context.write(healthy, text);
    }
  }

}
