package com.mageddo.tobby.dagger;

import com.mageddo.tobby.Tobby;

import com.mageddo.tobby.internal.utils.StringUtils;

import dagger.Module;
import dagger.Provides;

import static com.mageddo.tobby.Tobby.Config.TOBBY_RECORD_TABLE_NAME_PROP;

@Module
class ConfigModule {

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
