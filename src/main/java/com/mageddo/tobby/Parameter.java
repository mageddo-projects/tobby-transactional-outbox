package com.mageddo.tobby;

import java.time.LocalDateTime;

public enum Parameter {
  /**
   * Saved in {@link LocalDateTime#toString()} format
   */
  LAST_PROCESSED_TIMESTAMP,

  LOCK
}
