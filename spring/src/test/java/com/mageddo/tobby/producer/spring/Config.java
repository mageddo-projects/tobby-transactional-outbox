package com.mageddo.tobby.producer.spring;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@SpringBootApplication
public class Config {
  @Bean
  @Primary
  public KafkaProducerProvider fakeKafkaProducerProvider() {
    return new MockKafkaProducerProvider();
  }

}
