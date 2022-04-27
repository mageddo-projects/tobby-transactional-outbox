package com.mageddo.tobby.producer.spring;

import com.mageddo.tobby.Tobby;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EnableTobbyTransactionalOutbox
@SpringBootTest
@ExtendWith(SpringExtension.class)
@ActiveProfiles("custom-table")
class CustomTableTest {

  @Test
  void mustCustomizeRecordTableName(){

    // arrange
    // act
    // assert
    assertEquals("BATATA", System.getProperty(Tobby.Config.TOBBY_RECORD_TABLE_NAME_PROP));

  }


}
