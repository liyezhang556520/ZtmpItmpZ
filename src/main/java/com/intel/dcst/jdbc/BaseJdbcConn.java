package com.intel.dcst.jdbc;

import java.sql.SQLException;

public abstract class BaseJdbcConn implements JdbcConn {
  public abstract void connect() throws SQLException;
}
