package com.mageddo.tobby.producer.spring;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.producer.Producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

import templates.ProducerRecordTemplates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
