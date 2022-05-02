package com.mageddo.tobby.factory;

import com.mageddo.RecordRecordCustomTableDAO;
import com.mageddo.db.DB;
import com.mageddo.tobby.RecordDAOGeneric;
import com.mageddo.tobby.internal.utils.Threads;

public class DAOFactory {
  private DAOFactory() {
  }

  public static RecordRecordCustomTableDAO createRecordCustomTableDao(DB db){
    switch (db.getName()){
      default:
        return new RecordDAOGeneric(db, Threads.newPool(10));
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
