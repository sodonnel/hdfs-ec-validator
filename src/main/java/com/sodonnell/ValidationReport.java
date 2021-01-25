package com.sodonnell;

import org.apache.commons.collections.list.UnmodifiableList;

import java.util.ArrayList;
import java.util.List;

public class ValidationReport {

  private final List<Entry> validBlockGroups = new ArrayList<>();
  private final List<Entry> corruptBlockGroups = new ArrayList<>();
  private final List<Entry> zeroParityGroups = new ArrayList<>();

  public void addCorruptBlockGroup(String blkGroup, int stripesChecked) {
    corruptBlockGroups.add(new Entry(blkGroup, stripesChecked));
  }

  public void addValidBlockGroup(String blkGroup, int stripesChecked) {
    validBlockGroups.add(new Entry(blkGroup, stripesChecked));
  }

  public void addZeroParityBlockGroup(String blkGroup, int stripesChecked) {
    zeroParityGroups.add(new Entry(blkGroup, stripesChecked));
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

  public boolean isCorrupt() {
    return corruptBlockGroups.size() > 0;
  }

  public boolean isHealthy() {
    return corruptBlockGroups.size() == 0;
  }

  public boolean isParityAllZero() {
    return zeroParityGroups.size() != 0;
  }

  public static class Entry {

    private String block;
    private int stripesChecked;

    public Entry(String block, int stripesChecked) {
      this.block = block;
      this.stripesChecked = stripesChecked;
    }

    public String block() {
      return block;
    }

    public int stripesChecked() {
      return stripesChecked;
    }

    @Override
    public String toString() {
      return block;
    }

  }

}
