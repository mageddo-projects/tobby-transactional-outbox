package com.mageddo.tobby.producer.spring;

import com.mageddo.tobby.Tobby;

import lombok.Data;

import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "tobby.config")
public class TobbyConfigProperties {

  private String recordTableName = "TTO_RECORD";

  public Tobby.Config toConfig() {
    return Tobby.Config
        .builder()
        .recordTableName(this.recordTableName)
        .build();
  }

}
