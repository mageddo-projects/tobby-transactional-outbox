package com.mageddo.tobby.producer.spring;

import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource
public class TobbyProducerJMX {

  private final com.mageddo.tobby.producer.jmx.TobbyProducerJMX delegate;

  public TobbyProducerJMX(com.mageddo.tobby.producer.jmx.TobbyProducerJMX delegate) {
    this.delegate = delegate;
  }

  @ManagedOperation
  @ManagedOperationParameter(
      name = "recordsIdsList",
      description = "Lista dos ids da tto record no formato uuid separados por pipe sem espaco, nao passe dos "
          + "milhares por chamada para não sobrecarregar a memoria da app"
  )
  public String reSend(String recordsIdsList) {
    return this.delegate.reSend(recordsIdsList);
  }
}
