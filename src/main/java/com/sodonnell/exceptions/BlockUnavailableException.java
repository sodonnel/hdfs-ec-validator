package com.sodonnell.exceptions;

import java.io.IOException;

public class BlockUnavailableException extends IOException {
  public BlockUnavailableException() {
    super();
  }

  public BlockUnavailableException(String message) {
    super(message);
  }
  
  public BlockUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public BlockUnavailableException(Throwable cause) {
    super(cause);
  }
}
