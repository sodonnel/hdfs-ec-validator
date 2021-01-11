package com.sodonnell.exceptions;

import java.io.IOException;

public class NotErasureCodedException extends IOException {
  public NotErasureCodedException() {
    super();
  }

  public NotErasureCodedException(String message) {
    super(message);
  }

  public NotErasureCodedException(String message, Throwable cause) {
    super(message, cause);
  }

  public NotErasureCodedException(Throwable cause) {
    super(cause);
  }
}
