package com.mageddo.tobby.producer.spring;

import java.util.Map;
import java.util.UUID;

import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.producer.Producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@EnableTobbyTransactionalOutbox
@SpringBootApplication
@SpringBootTest
@ExtendWith(SpringExtension.class)
class ProducerSpringTest {

  @Autowired
  Producer producer;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Test
  void mustProduceMessagesSavingToDatabase(){
    // arrange
    final ProducerRecord record = new ProducerRecord(
        "fruit",
        "some key".getBytes(),
        "Grape".getBytes()
    );
    // act
    this.producer.send(record);

    // assert
    final Map<String, Object> foundRecord = this.jdbcTemplate.queryForMap(
        "SELECT * FROM TTO_RECORD"
    );
    System.out.println(foundRecord);
    assertNotNull(UUID.fromString((String) foundRecord.get("IDT_TTO_RECORD")));
    assertEquals("fruit", foundRecord.get("NAM_TOPIC"));
    assertEquals("c29tZSBrZXk=", foundRecord.get("TXT_KEY"));
    assertEquals("R3JhcGU=", foundRecord.get("TXT_VALUE"));
    assertNull(foundRecord.get("JSN_HEADERS"));
    assertNull(foundRecord.get("NUM_PARTITION"));
    assertNotNull(foundRecord.get("DAT_CREATED"));
  }

}
