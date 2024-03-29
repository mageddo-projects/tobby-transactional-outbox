package com.mageddo.tobby.producer.spring;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.dagger.TobbyFactory;
import com.mageddo.tobby.producer.Config;

import com.mageddo.tobby.producer.MockProducerProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import templates.ProducerRecordTemplates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@EnableTobbyTransactionalOutbox
@SpringBootTest(classes = Config.class)
@ExtendWith({SpringExtension.class})
class ProducerSpringTest {

  @SpyBean
  ProducerEventuallyConsistentSpring producer;

  @Autowired
  JdbcTemplate jdbcTemplate;

  @Autowired
  MockProducerProvider producerProvider;

  @Autowired
  TransactionalService transactionalService;

  @Test
  void mustInjectSpringProducerAsDefault() {
    // arrange

    // act

    // assert
    final var className = this.producer
        .getClass()
        .getSimpleName();
    assertTrue(className.startsWith(ProducerEventuallyConsistentSpring.class.getSimpleName()), className);
  }

  @Test
  void mustProduceMessagesSavingToDatabase() {
    // arrange
    final ProducerRecord record = ProducerRecordTemplates.grape();

    // act
    this.producer.send(record);

    // assert
    final Map<String, Object> foundRecord = this.jdbcTemplate.queryForMap(
        "SELECT * FROM TTO_RECORD"
    );
    assertNotNull(UUID.fromString((String) foundRecord.get("IDT_TTO_RECORD")));
    assertEquals("fruit", foundRecord.get("NAM_TOPIC"));
    assertEquals("c29tZSBrZXk=", foundRecord.get("TXT_KEY"));
    assertEquals("R3JhcGU=", foundRecord.get("TXT_VALUE"));
    assertNull(foundRecord.get("TXT_HEADERS"));
    assertNull(foundRecord.get("NUM_PARTITION"));
    assertNotNull(foundRecord.get("DAT_CREATED"));

    assertEquals(1, this.producerProvider.get().history().size());
  }

  @Test
  void mustCloseConnectionAutomaticallyWhenMethodWhichCalledWereNotTransactional() {
    // arrange
    final var capturedConnections = new ArrayList<Connection>();
    doAnswer(invocation -> {
      final var r = invocation.callRealMethod();
      capturedConnections.add((Connection) r);
      return r;
    })
        .when(this.producer)
        .getConnection();
    final var wantedInvocations = 3;
    final var record = ProducerRecordTemplates.grape();

    // act
    this.producer.send(record.copyWithNewId());

    // assert
    this.producer.send(record.copyWithNewId());
    this.producer.send(record.copyWithNewId());

    verify(this.producer, times(wantedInvocations)).getConnection();

    assertEquals(wantedInvocations, capturedConnections.size());
    assertEquals(wantedInvocations, new HashSet<>(capturedConnections).size());

  }

  @Test
  void mustReuseSameConnectionWhenSendIsRanInsideATransactionalMethod() {
    // arrange
    final var capturedConnections = new ArrayList<Connection>();
    doAnswer(invocation -> {
      final var r = invocation.callRealMethod();
      capturedConnections.add((Connection) r);
      return r;
    })
        .when(this.producer)
        .getConnection();
    final var wantedInvocations = 3;
    final var record = ProducerRecordTemplates.grape();

    // act
    this.transactionalService.send(record, wantedInvocations);

    // assert
    verify(this.producer, times(wantedInvocations)).getConnection();

    assertEquals(wantedInvocations, capturedConnections.size());
    assertEquals(1, new HashSet<>(capturedConnections).size());

  }


}
