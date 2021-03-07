package com.mageddo.tobby.factory;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordDAOGeneric;
import com.mageddo.tobby.internal.utils.DB;

public class DAOFactory {
  private DAOFactory() {
  }

  public static RecordDAO createRecordDao(DB db){
    switch (db){
      default:
        return new RecordDAOGeneric(db);
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
}
