package com.mageddo.tobby.producer.jdbi;

import java.util.Map;

import com.radcortez.flyway.test.annotation.DataSource;
import com.radcortez.flyway.test.annotation.FlywayTest;

import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import templates.ProducerRecordTemplates;

import static com.mageddo.tobby.producer.jdbi.ProducerJdbi3Test.DATA_SOURCE;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@FlywayTest(additionalLocations = "classpath:db/migration-hsqldb", value = @DataSource(url = DATA_SOURCE))
class ProducerJdbi3Test {

  public static final String DATA_SOURCE = "jdbc:hsqldb:mem:testdb";

  ProducerJdbi3 producerJdbi3;

  Jdbi jdbi;

  @BeforeEach
  void beforeEach() {
    this.jdbi = Jdbi.create(
        DATA_SOURCE,
        "",
        ""
    );
    this.producerJdbi3 = new ProducerJdbi3(this.jdbi);
  }

  @Test
  void mustPersistMessage() {

    // arrange
    final var record = ProducerRecordTemplates.strawberry();

    // act
    final var send = this.producerJdbi3.send(record);

    // assert
    assertNotNull(send);
    final Map<String, Object> found = this.jdbi.withHandle(handle -> handle
        .createQuery("SELECT * FROM TTO_RECORD")
        .mapToMap()
        .findOnly()
    );
    assertNotNull(found);

  }

}
