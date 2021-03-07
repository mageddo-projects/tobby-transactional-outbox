package com.mageddo.tobby.producer.spring;

import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.producer.Producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@EnableTobbyTransactionalOutbox
@SpringBootApplication
@SpringBootTest
@ExtendWith(SpringExtension.class)
class ProducerSpringTest {

  @Autowired
  Producer producer;

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
  }

}
