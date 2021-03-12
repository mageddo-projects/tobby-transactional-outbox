package com.mageddo.db;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(of = "name")
public class DB {

  private static final DB POSTGRESQL = new DB("POSTGRESQL");
  private static final DB SQLSERVER = new DB("SQLSERVER");
  private static final DB MYSQL = new DB("MYSQL");
  private static final DB SQLITE = new DB("SQLITE");
  private static final DB HSQLDB = new DB("HSQLDB");
  private static final DB H2 = new DB("H2");
  private static final DB ORACLE = new DB("ORACLE");

  String name;

  public static DB of(String name) {
    return new DB(name.toLowerCase());
  }
}
