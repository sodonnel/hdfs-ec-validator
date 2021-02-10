package com.sodonnell;

import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ValidationReport {

  private final String path;
  private final List<Entry> failedBlockGroups = new ArrayList<>();
  private final List<Entry> validBlockGroups = new ArrayList<>();
  private final List<Entry> corruptBlockGroups = new ArrayList<>();
  private final List<Entry> zeroParityGroups = new ArrayList<>();

  public ValidationReport(String path) {
    this.path = path;
  }

  public void addFailedBlockGroup(String blkGroup, int stripesChecked, String details) {
    failedBlockGroups.add(new Entry(blkGroup, stripesChecked, details));
  }

  public void addCorruptBlockGroup(String blkGroup, int stripesChecked) {
    corruptBlockGroups.add(new Entry(blkGroup, stripesChecked));
  }

  public void addValidBlockGroup(String blkGroup, int stripesChecked) {
    validBlockGroups.add(new Entry(blkGroup, stripesChecked));
  }

  public void addZeroParityBlockGroup(String blkGroup, int stripesChecked, String details) {
    zeroParityGroups.add(new Entry(blkGroup, stripesChecked, details));
  }

  public List<Entry> failedBlockGroups() {
    return UnmodifiableList.decorate(failedBlockGroups);
  }

  public List<Entry> validBlockGroups() {
    return UnmodifiableList.decorate(validBlockGroups);
  }

  public List<Entry> corruptBlockGroups() {
    return UnmodifiableList.decorate(corruptBlockGroups);
  }

  public List<Entry> parityAllZeroBlockGroups() {
    return UnmodifiableList.decorate(zeroParityGroups);
  }

  public boolean isFailed() {
    return failedBlockGroups.size() > 0;
  }

  public boolean isCorrupt() {
    return corruptBlockGroups.size() > 0 && failedBlockGroups.size() == 0;
  }

  public boolean isHealthy() {
    return corruptBlockGroups.size() == 0 && failedBlockGroups.size() == 0;
  }

  public boolean isParityAllZero() {
    return zeroParityGroups.size() != 0;
  }

  public String formatReport(String fs) {
    StringBuilder sb = new StringBuilder();
    String zeroParity = "" ;
    if (isFailed()) {
      sb.append("failed"+fs);
      sb.append(path+fs);
      sb.append("failed block groups {");
      sb.append(StringUtils.join(failedBlockGroups(), ","));
      sb.append("}");
    } else {
      if (isParityAllZero()) {
        zeroParity = "zeroParityBlockGroups {" + StringUtils.join(parityAllZeroBlockGroups(), ",")+"}";
      }
      if (isHealthy()) {
        sb.append("healthy" + fs + path + fs + zeroParity);
      } else {
        sb.append("corrupt" + fs + path + fs);
        sb.append("corrupt block groups {");
        sb.append(StringUtils.join(corruptBlockGroups(), ","));
        sb.append("}");
        if (zeroParity != "") {
          sb.append(" ");
          sb.append(zeroParity);
        }
      }
    }
    return sb.toString();
  }

  public static class Entry {

    private String block;
    private int stripesChecked;
    private String details = null;

    public Entry(String block, int stripesChecked) {
      this.block = block;
      this.stripesChecked = stripesChecked;
    }

    public Entry(String block, int stripesChecked, String details) {
      this.block = block;
      this.stripesChecked = stripesChecked;
      this.details = details;
    }

    public String block() {
      return block;
    }

    public int stripesChecked() {
      return stripesChecked;
    }

    public String details() {
      return details;
    }

    @Override
    public String toString() {
      if (details != null) {
        return block + " " + details;
      }
      return block;
    }

  }

}
