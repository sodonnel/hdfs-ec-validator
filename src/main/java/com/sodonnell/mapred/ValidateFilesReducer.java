package com.sodonnell.mapred;

import com.sodonnell.ECValidatorConfigKeys;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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

  private String fs = null;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    fs = conf.get(ECValidatorConfigKeys.ECVALIDATOR_FIELD_SEPARATOR_KEY,
        ECValidatorConfigKeys.ECVALIDATOR_FIELD_SEPARATOR_DEFAULT);
  }

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

    String file = text.toString();
    StringBuilder sb = new StringBuilder();
    if (firstFailureReason != null) {
      sb.append(firstFailureReason);
    }

    if (fileFailure) {
      context.write(failed, new Text(file + fs + sb.toString()));
      return;
    }

    appendBlocksWithMessage(sb, "corrupt block groups", corruptBlocks);
    appendBlocksWithMessage(sb, "zero parity block groups", zeroParity);
    appendBlocksWithMessage(sb, "failed block groups", failedBlocks);

    if (failedBlocks.size() > 0) {
      context.write(failed, new Text(file + fs + sb.toString()));
    } else if (corruptBlocks.size() > 0) {
      context.write(corrupt, new Text(file + fs + sb.toString()));
    } else {
      context.write(healthy, new Text(file + fs));
    }
  }

  private void appendBlocksWithMessage(StringBuilder sb, String msg, List<String> blocks) {
    if (blocks.size() == 0) {
      return;
    }
    if (sb.length() > 0) {
      sb.append(" ");
    }
    sb.append(msg + " {");
    sb.append(StringUtils.join(blocks, ","));
    sb.append("}");
  }

}
