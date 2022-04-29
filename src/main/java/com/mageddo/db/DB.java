package com.mageddo.db;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(of = "name")
public class DB {

  public static final DB POSTGRESQL = new DB("POSTGRESQL");
  public static final DB SQLSERVER = new DB("SQLSERVER");
  public static final DB MYSQL = new DB("MYSQL");
  public static final DB SQLITE = new DB("SQLITE");
  public static final DB HSQLDB = new DB("HSQLDB");
  public static final DB H2 = new DB("H2");
  public static final DB ORACLE = new DB("ORACLE");

  String name;

  public static DB of(String name) {
    return new DB(name.toLowerCase());
  }
}
