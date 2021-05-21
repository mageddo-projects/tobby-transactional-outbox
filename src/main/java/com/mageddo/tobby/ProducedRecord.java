package com.mageddo.tobby;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ProducedRecord {

  private UUID id;

  /**
   * https://stackoverflow.com/a/37067544/2979435
   */
  private String topic;
  private Integer partition;
  private byte[] key;
  private byte[] value;
  private Headers headers;
  private LocalDateTime createdAt;

  public static List<UUID> toIds(List<ProducedRecord> records) {
    return records
        .stream()
        .map(ProducedRecord::getId)
        .collect(Collectors.toList());
  }

  public enum Status {
    OPEN,
    DONE,
  }
}
