package com.sodonnell.exceptions;

import java.io.IOException;

public class MisalignedBuffersException extends IOException {
  public MisalignedBuffersException() {
    super();
  }

  public MisalignedBuffersException(String message) {
    super(message);
  }

  public MisalignedBuffersException(String message, Throwable cause) {
    super(message, cause);
  }

  public MisalignedBuffersException(Throwable cause) {
    super(cause);
  }
}
