package com.sodonnell.mapred;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BlockReport implements WritableComparable<BlockReport> {

  private String blockGroup;
  private int stripesChecked = 0;
  private boolean isCorrupt;
  private boolean hasZeroParity;
  private boolean failed;
  private String message = "";

  public BlockReport setBlockGroup(String blockGroup) {
    this.blockGroup = blockGroup;
    return this;
  }

  public String blockGroup() {
    return blockGroup;
  }

  public BlockReport setStripesChecked(int stripesChecked) {
    this.stripesChecked = stripesChecked;
    return this;
  }

  public int stripesChecked() {
    return stripesChecked;
  }

  public BlockReport setIsCorrupt(boolean isCorrupt) {
    this.isCorrupt = isCorrupt;
    return this;
  }

  public boolean isCorrupt() {
    return isCorrupt;
  }

  public BlockReport setHasZeroParity(boolean zeroParity) {
    this.hasZeroParity = zeroParity;
    return this;
  }

  public boolean hasZeroParity() {
    return hasZeroParity;
  }

  public BlockReport setFailed(boolean failed) {
    this.failed = failed;
    return this;
  }

  public boolean failed() {
    return failed;
  }

  public BlockReport setMessage(String msg) {
    this.message = msg;
    return this;
  }

  public String message() {
    return message;
  }

  public void clear() {
    blockGroup = null;
    stripesChecked = 0;
    isCorrupt = false;
    hasZeroParity = false;
    failed = false;
    message = "";
  }

  @Override
  public int compareTo(BlockReport o) {
    return blockGroup.compareTo(o.blockGroup());
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(blockGroup);
    dataOutput.writeInt(stripesChecked);
    dataOutput.writeBoolean(isCorrupt);
    dataOutput.writeBoolean(hasZeroParity);
    dataOutput.writeBoolean(failed);
    dataOutput.writeUTF(message);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    blockGroup = dataInput.readUTF();
    stripesChecked = dataInput.readInt();
    isCorrupt = dataInput.readBoolean();
    hasZeroParity = dataInput.readBoolean();
    failed = dataInput.readBoolean();
    message = dataInput.readUTF();
  }
}
