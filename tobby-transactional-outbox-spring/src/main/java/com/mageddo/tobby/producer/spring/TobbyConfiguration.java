package com.mageddo.tobby.producer.spring;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnExpression(value =
    "${tobby.transactional.outbox.enabled} == 'true' || ${tobby.transactional.outbox.enabled} == null"
)
public class TobbyConfiguration {

  @Bean
  public ProducerSpring producerSpring(DataSource dataSource){
    return new ProducerSpring(dataSource);
  }
}
