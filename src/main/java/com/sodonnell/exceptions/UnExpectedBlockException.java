package com.sodonnell.exceptions;

import java.io.IOException;

public class UnExpectedBlockException extends IOException {
  public UnExpectedBlockException() {
    super();
  }

  public UnExpectedBlockException(String message) {
    super(message);
  }

  public UnExpectedBlockException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnExpectedBlockException(Throwable cause) {
    super(cause);
  }
}
