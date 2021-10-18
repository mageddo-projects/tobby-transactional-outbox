package com.mageddo.tobby.producer.kafka;

import java.util.Map;
import java.util.UUID;

import com.mageddo.tobby.producer.Config;
import com.mageddo.tobby.producer.spring.EnableTobbyTransactionalOutbox;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@EnableKafka
@EnableTobbyTransactionalOutbox
@SpringBootTest(classes = Config.class)
@ExtendWith(SpringExtension.class)
public class KafkaTemplateTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  JdbcTemplate jdbcTemplate;

  @Test
  void mustConfigureKafkaProducerAdapter() {
    // arrange

    // act
    this.kafkaTemplate.send("fruit", "Orange");

    // assert
    final Map<String, Object> foundRecord = this.jdbcTemplate.queryForMap(
        "SELECT * FROM TTO_RECORD"
    );
    assertNotNull(UUID.fromString((String) foundRecord.get("IDT_TTO_RECORD")));
    assertEquals("fruit", foundRecord.get("NAM_TOPIC"));
    assertEquals("T3Jhbmdl", foundRecord.get("TXT_VALUE"));
    assertNull(foundRecord.get("TXT_HEADERS"));
    assertNull(foundRecord.get("NUM_PARTITION"));
    assertNotNull(foundRecord.get("DAT_CREATED"));
  }

}
