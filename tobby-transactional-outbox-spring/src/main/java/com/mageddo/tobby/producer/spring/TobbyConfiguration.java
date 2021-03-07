package com.mageddo.tobby.producer.spring;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.factory.DAOFactory;
import com.mageddo.tobby.internal.utils.DBUtils;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
//@ConditionalOnExpression(value =
//    "#{ ${tobby.transactional.outbox.enabled:true} == 'true' }"
//)
public class TobbyConfiguration {

  @Bean
  public ProducerSpring producerSpring(RecordDAO recordDAO, DataSource dataSource) {
    return new ProducerSpring(recordDAO, dataSource);
  }

  @Bean
  public RecordDAO recordDAOHsqldb(DataSource dataSource){
    try(Connection connection = dataSource.getConnection()){
      return DAOFactory.createRecordDao(DBUtils.discoverDB(connection));
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }
}
