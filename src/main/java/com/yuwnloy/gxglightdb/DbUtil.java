package com.yuwnloy.gxglightdb;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

public class DbUtil {
	private DataSource dataSource;
	private QueryRunner runner = null;
	private TranQueryRunner tranRunner = null;
	/**
	 * is open Transaction
	 */
	private boolean openTran = false;
	protected DbUtil(){
		this.dataSource = new ComboPooledDataSource();
		this.runner = new QueryRunner(dataSource);
		this.tranRunner = new TranQueryRunner(dataSource);
	}
	protected DbUtil(String configFileName) throws DbUtilException{
		this.loadProperties(configFileName);
		this.runner = new QueryRunner(dataSource);
		this.tranRunner = new TranQueryRunner(dataSource);
	}
	/**
	 * load the c3p0 config files.
	 * @throws RongDbException 
	 */
	private void loadProperties(String configFile) throws DbUtilException{
		if(this.getDataSource()!=null){
			try {
				DataSources.destroy(this.getDataSource());
			} catch (SQLException e) {
				throw new DbUtilException("Error when destroy old datasource.[SQLException:"+e.getMessage()+"]");
			}
		}
		//load properties file
		Properties properties = new Properties();
		InputStream is = DbUtil.class.getClassLoader().getResourceAsStream(configFile);
		try {
			properties.load(is);
		} catch (IOException e) {
			throw new DbUtilException("Error when load property file '"+configFile+"'[IOException:"+e.getMessage()+"].");
		}
		//set the properties
		ComboPooledDataSource ds = new ComboPooledDataSource();
		String driverClass = properties.getProperty("driverClass");
		if(driverClass!=null){
			try {
				ds.setDriverClass(properties.getProperty(driverClass));
			} catch (PropertyVetoException e) {
				throw new DbUtilException("Error when set db driver class '"+driverClass+"'[PropertyVetoException:"+e.getMessage()+"].");
			}
		}
		ds.setJdbcUrl(properties.getProperty("jdbcUrl"));
		ds.setUser(properties.getProperty("user"));
		ds.setPassword(properties.getProperty("password"));
		ds.setMinPoolSize(DataHelper.ToInt(properties.getProperty("minPoolSize")));
		ds.setMaxPoolSize(DataHelper.ToInt(properties.getProperty("maxPoolSize")));
		ds.setAcquireIncrement(DataHelper.ToInt(properties.getProperty("acquireIncrement")));
		ds.setAcquireRetryAttempts(DataHelper.ToInt(properties.getProperty("acquireRetryAttempts")));
		ds.setAcquireRetryDelay(DataHelper.ToInt(properties.getProperty("acquireRetryDelay")));
		ds.setAutoCommitOnClose(DataHelper.ToBoolean(properties.getProperty("autoCommitOnClose")));
		ds.setCheckoutTimeout(DataHelper.ToInt(properties.getProperty("checkoutTimeout")));
		ds.setIdleConnectionTestPeriod(DataHelper.ToInt(properties.getProperty("idleConnectionTestPeriod")));
		ds.setMaxIdleTime(DataHelper.ToInt(properties.getProperty("maxIdleTime")));
		ds.setTestConnectionOnCheckin(DataHelper.ToBoolean(properties.getProperty("testConnectionOnCheckin")));
		ds.setMaxStatements(DataHelper.ToInt(properties.getProperty("maxStatements")));
		ds.setMaxStatementsPerConnection(DataHelper.ToInt(properties.getProperty("maxStatementsPerConnection")));
		this.setDataSource(ds);
	}
	/**
	 * query one row as a bean.
	 * @param cls
	 * @param sql
	 * @param params
	 * @return
	 */
	public <T> T query2Bean(Class<T> cls, String sql,Object... params)throws SQLException{
		ResultSetHandler<T> handler = new BeanHandler<T>(cls);
		T obj = this.runner.query(sql, handler, params);
		return obj;
	}
	
	/**
	 * query one row as a bean. 
	 * @param cls
	 * @param conn
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public <T> T query2Bean(Class<T> cls, Connection conn, String sql,Object... params)throws SQLException{
		ResultSetHandler<T> handler = new BeanHandler<T>(cls);
		T obj = this.runner.query(conn, sql, handler, params);
		return obj;
	}
	/**
	 * query one row as an array
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public Object[] query2Array(String sql, Object... params) throws SQLException{
		ResultSetHandler<Object[]> handler = new ArrayHandler();
		Object[] array = this.runner.query(sql, handler, params);
		return array;
	}
	public Object[] query2Array(Connection conn, String sql, Object... params)throws SQLException{
		ResultSetHandler<Object[]> handler = new ArrayHandler();
		return this.runner.query(conn, sql, handler, params);
	}
	
	/**
	 * query one row as a map.
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public Map<String, Object> query2Map(String sql, Object... params) throws SQLException{
		ResultSetHandler<Map<String, Object>> handler = new MapHandler();
		return this.runner.query(sql, handler, params);
	}
	public Map<String, Object> query2Map(Connection conn, String sql, Object... params) throws SQLException{
		ResultSetHandler<Map<String, Object>> handler = new MapHandler();
		return this.runner.query(conn, sql, handler, params);
	}
	
	/**
	 * query a set of row as a bean list.
	 * @param cls
	 * @param sql
	 * @param params
	 * @return
	 */
 	public <T> List<T> query2BeanList(Class<T> cls, String sql,Object... params)throws SQLException{
 		ResultSetHandler<T> handler = new BeanListHandler(cls);
 		List<T> list = (List<T>)this.runner.query(sql, handler, params);
		return list;
	}
	public <T> List<T> query2BeanList(Class<T> cls, Connection conn, String sql,Object... params)throws SQLException{
		ResultSetHandler<T> handler = new BeanListHandler(cls);
		List<T> list = (List<T>)this.runner.query(conn, sql, handler, params);
		return list;
	}
	
	/**
	 * query a set of row as a array list.
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public List<Object[]> query2ArrayList(String sql, Object...params) throws SQLException{
		ResultSetHandler<List<Object[]>> handler = new ArrayListHandler();
		return this.runner.query(sql, handler,params);
	}
	public List<Object[]> query2ArrayList(Connection conn,String sql, Object...params) throws SQLException{
		ResultSetHandler<List<Object[]>> handler = new ArrayListHandler();
		return this.runner.query(conn, sql, handler,params);
	}
	
	/**
	 * query a set of row as a map list.
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public List<Map<String, Object>> query2MapList(String sql, Object... params) throws SQLException{
		ResultSetHandler<List<Map<String, Object>>> handler = new MapListHandler();
		return this.runner.query(sql, handler, params);
	}
	public List<Map<String, Object>> query2MapList(Connection conn, String sql, Object... params) throws SQLException{
		ResultSetHandler<List<Map<String, Object>>> handler = new MapListHandler();
		return this.runner.query(conn, sql, handler, params);
	}
	
	/**
	 * query one column from a set of rows.
	 * @param sql
	 * @param columnIndex
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public List<Object> queryOneColumnList(String sql, int columnIndex, Object...params) throws SQLException{
		ResultSetHandler<List<Object>> handler = new ColumnListHandler(columnIndex);
		return this.runner.query(sql, handler,params);
	}
	
	
	
	public List<Object> queryOneColumnList(Connection conn, String sql, int columnIndex, Object...params) throws SQLException{
		ResultSetHandler<List<Object>> handler = new ColumnListHandler(columnIndex);
		return this.runner.query(conn, sql, handler, params);
	}
	
	public List<Object> queryOneColumnList(String sql, String columnName, Object...params) throws SQLException{
		ResultSetHandler<List<Object>> handler = new ColumnListHandler(columnName);
		return this.runner.query(sql, handler,params);
	}
	
	
	public List<Object> queryOneColumnList(Connection conn, String sql, String columnName, Object...params) throws SQLException{
		ResultSetHandler<List<Object>> handler = new ColumnListHandler(columnName);
		return this.runner.query(conn, sql, handler, params);
	}
	/**
	 * update begin
	 */
	
	/**
	 * 
	 * @param conn
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public int update(Connection conn, String sql, Object... params) throws SQLException{
		if(this.isOpenTran()){
			return this.tranRunner.update(conn, sql, params);
		}else{
			return this.runner.update(conn, sql, params);
		}
	}
	/**
	 * 
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public int update(String sql, Object... params) throws SQLException{
		if(this.isOpenTran()){
			return this.tranRunner.update(sql, params);
		}else{
			return this.runner.update(sql, params);
		}
	}
	
	public int[] updateBatch(Connection conn, String sql, Object[][] params) throws SQLException{
		if(this.isOpenTran()){
			return this.tranRunner.batch(conn, sql, params);
		}else{
			return this.runner.batch(conn, sql, params);
		}
	}

	public int[] updateBatch(String sql, Object[][] params) throws SQLException {
		if(this.isOpenTran()){
			return this.tranRunner.batch(sql, params);
		}else{
			return this.runner.batch(sql, params);
		}
	}
	/**
	 * update end
	 */
	/**
	 * delete begin
	 */
	public int delete(Connection conn, String sql, Object... params) throws SQLException{
		return this.update(conn, sql, params);
	}
	public int delete(String sql, Object... params)throws SQLException{
		return this.update(sql, params);
	}
	
	public int[] deleteBatch(Connection conn, String sql, Object[][] params) throws SQLException{
		return this.updateBatch(conn, sql, params);
	}
	public int[] deleteBatch(String sql, Object[][] params) throws SQLException {
		return this.updateBatch(sql, params);
	}
	/**
	 * delete end
	 */
	/**
	 * insert begin
	 */
	public int insert(Connection conn, String sql, Object... params)throws SQLException{
		return this.update(conn, sql, params);
	}
	public int insert(String sql, Object... params)throws SQLException{
		return this.update(sql, params);
	}
	
	public int[] insertBatch(Connection conn, String sql, Object[][] params) throws SQLException{
		return this.updateBatch(conn, sql, params);
	}
	public int[] insertBatch(String sql, Object[][] params) throws SQLException {
		return this.updateBatch(sql, params);
	}
	/**
	 * insert end
	 */
	public boolean isOpenTran() {
		return openTran;
	}
	public void setOpenTran(boolean openTran) {
		this.openTran = openTran;
	}
	public DataSource getDataSource() {
		return dataSource;
	}
	protected void setDataSource(DataSource ds){
		this.dataSource = ds;
	}
	/**
	 * Immediately releases resources (Threads and database Connections) that are held by a C3P0 DataSource.
	 * @throws SQLException
	 */
	public void destroy() throws SQLException{
		if(this.getDataSource()!=null)
			DataSources.destroy(this.dataSource);
	}
}
