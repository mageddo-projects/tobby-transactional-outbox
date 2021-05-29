package com.mageddo.tobby.dagger;

import com.mageddo.tobby.Tobby;

import com.mageddo.tobby.internal.utils.StringUtils;

import dagger.Module;
import dagger.Provides;

@Module
class ConfigModule {

  public static final String TOBBY_RECORD_TABLE_NAME_PROP = "tobby.record-table.name";
  private final Tobby.Config config;

  ConfigModule(Tobby.Config config) {
    this.config = config;
  }

  @Provides
  public Tobby.Config config() {
    if (!this.recordTableAlreadySet()) {
      System.setProperty(TOBBY_RECORD_TABLE_NAME_PROP, this.config.getRecordTableName());
    }
    return this.config;
  }

  private boolean recordTableAlreadySet() {
    return System.getProperties()
        .contains(TOBBY_RECORD_TABLE_NAME_PROP);
  }

}
