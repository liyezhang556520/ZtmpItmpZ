package com.intel.dcst.jdbc;

import java.sql.SQLException;

public interface JdbcConn {
  public void connect() throws SQLException;
}
