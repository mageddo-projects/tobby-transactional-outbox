package com.mageddo.tobby;

import java.time.LocalDateTime;
import java.util.UUID;

import lombok.Builder;
import lombok.Data;
import lombok.Value;
import lombok.experimental.Accessors;

@Data
@Builder
@Accessors(chain = true)
public class ProducedRecord {

  private final UUID id;

  /**
   * https://stackoverflow.com/a/37067544/2979435
   */
  private final String topic;
  private final Integer partition;
  private final byte[] key;
  private final byte[] value;
  private final Headers headers;

  private final LocalDateTime createdAt;

  private Status status;

  /**
   * Partition kafka server informed where sent record to.
   */
  private Integer sentPartition;

  /**
   * Offset kafka server informed where sent record to.
   */
  private Long sentOffset;

  public enum Status {
    WAIT,
    OK
  }

}
