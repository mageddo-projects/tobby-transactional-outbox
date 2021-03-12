package com.mageddo.db;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class SimpleDataSource implements DataSource {

  private String url;
  private String username;
  private String password;
  private PrintWriter logWriter;

  public SimpleDataSource(String url) {
    this(url, null, null);
  }

  public SimpleDataSource(String url, PrintWriter logWriter) {
    this(url, null, null, logWriter);
  }

  public SimpleDataSource(String url, String username, String password) {
    this(url, username, password, new PrintWriter(System.out));
  }

  public SimpleDataSource(String url, String username, String password, PrintWriter logWriter) {
    this.url = url;
    this.username = username;
    this.password = password;
    this.logWriter = logWriter;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return DriverManager.getConnection(this.url, this.username, this.password);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return DriverManager.getConnection(this.url, username, password);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    this.logWriter = out;
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {

  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }
}
