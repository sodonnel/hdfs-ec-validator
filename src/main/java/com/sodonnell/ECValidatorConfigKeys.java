package com.sodonnell;

public class ECValidatorConfigKeys {

  public static String ECVALIDATOR_VERIFY_CHECKSUMS = "ecvalidate.verify.checksums";
  public static boolean ECVALIDATOR_VERIFY_CHECKSUMS_DEFAULT = true;

  public static String ECVALIDATOR_READ_TIMEOUT = "ecvalidate.read.timeout.seconds";
  public static int ECVALIDATOR_READ_TIMEOUT_DEFAULT = 70;

  public static String ECVALIDATOR_FIELD_SEPARATOR_KEY = "ecvalidate.file.separator";
  public static String ECVALIDATOR_FIELD_SEPARATOR_DEFAULT = ";";
}
