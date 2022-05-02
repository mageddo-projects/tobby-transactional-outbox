package com.mageddo.tobby.examples.spring;

import com.mageddo.tobby.producer.spring.EnableTobbyTransactionalOutbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableTobbyTransactionalOutbox
public class App {
  public static void main(String[] args) {
    SpringApplication.run(App.class, args);
  }
}
