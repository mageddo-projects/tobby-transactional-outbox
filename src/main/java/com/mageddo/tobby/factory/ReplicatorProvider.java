package com.mageddo.tobby.factory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.tobby.replicator.IteratorFactory;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.ReplicatorConfig.ReplicatorConfigBuilder;
import com.mageddo.tobby.replicator.Replicators;

@Singleton
public class ReplicatorProvider {

  private final DataSource dataSource;
//  private final RecordDAO recordDAO;
//  private final ParameterDAO parameterDAO;
//  private final RecordProcessedDAO recordProcessedDAO;
  private final IteratorFactory iteratorFactory;

  @Inject
  public ReplicatorProvider(DataSource dataSource, IteratorFactory iteratorFactory) {
    this.dataSource = dataSource;
    this.iteratorFactory = iteratorFactory;
  }

  public Replicators create(ReplicatorConfig config) {
    final ReplicatorConfigBuilder builder = config.toBuilder();
    if(config.getDataSource() == null){
      builder.dataSource(this.dataSource);
    }
//    if(config.getRecordDAO() == null){
//      builder.recordDAO(this.recordDAO);
//    }
//    if(config.getRecordProcessedDAO() == null){
//      builder.recordProcessedDAO(this.recordProcessedDAO);
//    }
//    if(config.getParameterDAO() == null){
//      builder.parameterDAO(this.parameterDAO);
//    }
    return new Replicators(builder.build(), this.iteratorFactory);
  }
}
