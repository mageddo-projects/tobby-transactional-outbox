package com.mageddo.tobby.factory;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordDAOGeneric;
import com.mageddo.db.DB;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class DAOFactory {
  private DAOFactory() {
  }

  public static RecordDAO createRecordDao(DB db){
    switch (db.getName()){
      default:
        return new RecordDAOGeneric(db, buildPool());
//      case ORACLE:
//      case POSTGRES:
//        throw new UnsupportedOperationException();
//      case H2:
//      case HSQLDB:
//        return new RecordDAOHsqldb();
//      case MYSQL:
//      case SQLITE:
//      case SQLSERVER:
//        throw new UnsupportedOperationException();
    }
  }

  private static ScheduledExecutorService buildPool() {
    return Executors.newScheduledThreadPool(10, r -> {
      final Thread t = Executors
          .defaultThreadFactory()
          .newThread(r);
      t.setDaemon(true);
      return t;
    });
  }
}
