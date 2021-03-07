package templates;

import java.util.UUID;

import com.mageddo.tobby.ProducedRecord;

public class ProducedRecordTemplates {
  public static ProducedRecord coconut(){
    return new ProducedRecord(
        UUID.randomUUID(),
        "fruit",
        null,
        "GreenFruits".getBytes(),
        "Coconuts".getBytes(),
        null,
        null
    );
  }
}
