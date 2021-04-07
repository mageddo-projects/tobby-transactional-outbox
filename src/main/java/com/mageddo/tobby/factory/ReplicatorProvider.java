package com.mageddo.tobby.factory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.tobby.Locker;
import com.mageddo.tobby.replicator.IteratorFactory;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.ReplicatorConfig.ReplicatorConfigBuilder;
import com.mageddo.tobby.replicator.Replicators;

@Singleton
public class ReplicatorProvider {

  private final DataSource dataSource;
  private final IteratorFactory iteratorFactory;
  private final Locker locker;

  @Inject
  public ReplicatorProvider(DataSource dataSource, IteratorFactory iteratorFactory, Locker locker) {
    this.dataSource = dataSource;
    this.iteratorFactory = iteratorFactory;
    this.locker = locker;
  }

  public Replicators create(ReplicatorConfig config) {
    final ReplicatorConfigBuilder builder = config.toBuilder();
    if(config.getDataSource() == null){
      builder.dataSource(this.dataSource);
    }
    return new Replicators(builder.build(), this.iteratorFactory, this.locker);
  }
}
