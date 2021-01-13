package com.sodonnell;

import org.apache.commons.collections.list.UnmodifiableList;

import java.util.ArrayList;
import java.util.List;

public class ValidationReport {

  private final List<String> validBlockGroups = new ArrayList<>();
  private final List<String> corruptBlockGroups = new ArrayList<>();

  public void addCorruptBlockGroup(String blkGroup) {
    corruptBlockGroups.add(blkGroup);
  }

  public void addValidBlockGroup(String blkGroup) {
    validBlockGroups.add(blkGroup);
  }

  public List<String> validBlockGroups() {
    return UnmodifiableList.decorate(validBlockGroups);
  }

  public List<String> corruptBlockGroups() {
    return UnmodifiableList.decorate(corruptBlockGroups);
  }

  public boolean isCorrupt() {
    return corruptBlockGroups.size() > 0;
  }

  public boolean isHealthy() {
    return corruptBlockGroups.size() == 0;
  }

}
