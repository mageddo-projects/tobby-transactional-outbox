package com.mageddo.tobby.examples.spring;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FruitController {

  private final KafkaTemplate kafkaTemplate;

  public FruitController(KafkaTemplate kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @PostMapping("/api/fruits/tables")
  @ResponseBody
  public String putFruitOnTable(@RequestBody String name){
    this.kafkaTemplate.send(new ProducerRecord<>("fruit-topic", String.format("put a %s on the table", name)));
    return String.format("success for %s", name);
  }
}
