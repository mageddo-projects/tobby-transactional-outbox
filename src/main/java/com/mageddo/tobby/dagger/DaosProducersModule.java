package com.mageddo.tobby.dagger;

import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.RecordRecordCustomTableDAO;
import com.mageddo.db.DB;
import com.mageddo.db.DBUtils;
import com.mageddo.db.SqlErrorCodes;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.factory.DAOFactory;

import dagger.Module;
import dagger.Provides;

@Module
class DaosProducersModule {

  private final DataSource dataSource;

  DaosProducersModule(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Provides
  @Singleton
  DB db() {
    final DB db = DBUtils.discoverDB(this.dataSource);
    SqlErrorCodes.build(db);
    return db;
  }

  @Provides
  @Singleton
  public RecordDAO recordDAO(RecordRecordCustomTableDAO recordTableDAO) {
    return recordTableDAO;
  }

  @Provides
  @Singleton
  public RecordRecordCustomTableDAO recordRecordCustomTableDAO(DB db) {
    return DAOFactory.createRecordCustomTableDao(db);
  }

}
