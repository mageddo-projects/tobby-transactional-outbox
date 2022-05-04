package com.mageddo.tobby.producer;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@SpringBootApplication
public class Config {

  @Bean
  public MockProducerProvider mockProducer(){
    return new MockProducerProvider();
  }

  @Primary
  @Bean
  public ProducerConfig producerConfig2(DataSource dataSource, MockProducerProvider producerProvider){
    return ProducerConfig.builder()
        .dataSource(dataSource)
        .producer(producerProvider.get())
        .build();
  }

}
