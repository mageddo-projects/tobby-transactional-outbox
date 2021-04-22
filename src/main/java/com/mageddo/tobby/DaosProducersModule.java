package com.mageddo.tobby;

import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.db.DB;
import com.mageddo.db.DBUtils;
import com.mageddo.db.SqlErrorCodes;
import com.mageddo.tobby.factory.DAOFactory;
import com.mageddo.tobby.producer.ProducerJdbc;

import dagger.Module;
import dagger.Provides;

@Module
public class DaosProducersModule {

  private final DataSource dataSource;

  public DaosProducersModule(DataSource dataSource) {
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
  public RecordDAO recordDAO(DB db) {
    return DAOFactory.createRecordDao(db);
  }

  @Provides
  @Singleton
  ProducerJdbc producerJdbc(RecordDAO recordDAO) {
    return new ProducerJdbc(recordDAO, this.dataSource);
  }

}
