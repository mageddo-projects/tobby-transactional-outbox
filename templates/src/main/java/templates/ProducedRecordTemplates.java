package templates;

import java.util.UUID;

import com.mageddo.tobby.ProducedRecord;

public class ProducedRecordTemplates {
  public static ProducedRecord coconut() {
    return ProducedRecord
        .builder()
        .id(UUID.randomUUID())
        .topic("fruit")
        .key("GreenFruits".getBytes())
        .value("Coconuts".getBytes())
        .build();
  }
}
