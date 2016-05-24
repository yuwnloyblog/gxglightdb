package com.yuwnloy.gxglightdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;

public class TranQueryRunner extends QueryRunner{
	public TranQueryRunner(DataSource dataSource) {
		super(dataSource);
	}
	@Override
	public int update(Connection conn, String sql) throws SQLException {
        return this.update(conn, false, sql, (Object[]) null);
    }

	@Override
    public int update(Connection conn, String sql, Object param) throws SQLException {
        return this.update(conn, false, sql, new Object[]{param});
    }

	@Override
    public int update(Connection conn, String sql, Object... params) throws SQLException {
        return update(conn, false, sql, params);
    }

	@Override
    public int update(String sql) throws SQLException {
        Connection conn = this.prepareConnection();

        return this.update(conn, true, sql, (Object[]) null);
    }

	@Override
    public int update(String sql, Object param) throws SQLException {
        Connection conn = this.prepareConnection();

        return this.update(conn, true, sql, new Object[]{param});
    }

	@Override
    public int update(String sql, Object... params) throws SQLException {
        Connection conn = this.prepareConnection();

        return this.update(conn, true, sql, params);
    }

	@Override
	public int[] batch(Connection conn, String sql, Object[][] params) throws SQLException {
		return this.batch(conn, false, sql, params);
	}
	
	@Override
	public int[] batch(String sql, Object[][] params) throws SQLException {
        Connection conn = this.prepareConnection();
        return this.batch(conn, true, sql, params);
    }
	
	/**
     * Calls update after checking the parameters to ensure nothing is null.
     * @param conn The connection to use for the batch call.
     * @param closeConn True if the connection should be closed, false otherwise.
     * @param sql The SQL statement to execute.
     * @param params An array of query replacement parameters.  Each row in
     * this array is one set of batch replacement values.
     * @return The number of rows updated in the batch.
     * @throws SQLException If there are database or parameter errors.
     */
    private int[] batch(Connection conn, boolean closeConn, String sql, Object[][] params) throws SQLException {
        if (conn == null) {
            throw new SQLException("Null connection");
        }

        if (sql == null) {
            if (closeConn) {
                close(conn);
            }
            throw new SQLException("Null SQL statement");
        }

        if (params == null) {
            if (closeConn) {
                close(conn);
            }
            throw new SQLException("Null parameters. If parameters aren't need, pass an empty array.");
        }

        PreparedStatement stmt = null;
        int[] rows = null;
        boolean originAutoCommit = conn.getAutoCommit();
        try {
        	if(originAutoCommit)
        		conn.setAutoCommit(false);
            stmt = this.prepareStatement(conn, sql);

            for (int i = 0; i < params.length; i++) {
                this.fillStatement(stmt, params[i]);
                stmt.addBatch();
            }
            rows = stmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
        	conn.rollback();
            this.rethrow(e, sql, (Object[])params);
        } finally {
        	if(originAutoCommit)
        		conn.setAutoCommit(originAutoCommit);
            close(stmt);
            if (closeConn) {
                close(conn);
            }
        }

        return rows;
    }
    
    /**
     * Calls update after checking the parameters to ensure nothing is null.
     * @param conn The connection to use for the update call.
     * @param closeConn True if the connection should be closed, false otherwise.
     * @param sql The SQL statement to execute.
     * @param params An array of update replacement parameters.  Each row in
     * this array is one set of update replacement values.
     * @return The number of rows updated.
     * @throws SQLException If there are database or parameter errors.
     */
    private int update(Connection conn, boolean closeConn, String sql, Object... params) throws SQLException {
        if (conn == null) {
            throw new SQLException("Null connection");
        }

        if (sql == null) {
            if (closeConn) {
                close(conn);
            }
            throw new SQLException("Null SQL statement");
        }

        PreparedStatement stmt = null;
        int rows = 0;
        boolean originAutoCommit = conn.getAutoCommit();
        try {
        	if(originAutoCommit)
        		conn.setAutoCommit(false);
            stmt = this.prepareStatement(conn, sql);
            this.fillStatement(stmt, params);
            rows = stmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
        	conn.rollback();
            this.rethrow(e, sql, params);

        } finally {
        	if(originAutoCommit)
        		conn.setAutoCommit(originAutoCommit);
            close(stmt);
            if (closeConn) {
                close(conn);
            }
        }

        return rows;
    }
}
