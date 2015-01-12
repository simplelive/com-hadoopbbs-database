package com.datagreatwall.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;

/**
 * 
 * 数据库访问
 * 
 * @author 石瑜
 * 
 */

public class Database {

	public static Vector POOL = new Vector(); // 连接池

	public static int SIZE = 2; // 连接池空闲连接数，大多数系统设置为2即可，特别繁忙系统可设为3

	public static String DRIVER; // 驱动类

	public static String URL; // 连接地址

	public static String USER; // 用户名

	public static String PASSWORD; // 密码

	static {

		Properties prop = new Properties(); // 配置属性

		try {

			// 读取配置属性
			prop.load(Database.class.getResourceAsStream("Database.properties"));

		} catch (IOException ex) {

			throw new RuntimeException(ex);

		}

		try {

			SIZE = Integer.valueOf(prop.getProperty("SIZE").trim());

		} catch (Exception ex) {

			SIZE = 2;

		}

		DRIVER = prop.getProperty("DRIVER").trim();

		URL = prop.getProperty("URL").trim();

		USER = prop.getProperty("USER").trim();

		PASSWORD = prop.getProperty("PASSWORD").trim();

		try {

			Class.forName(DRIVER).newInstance();// 加载实例化驱动类

		} catch (Exception ex) {

			throw new RuntimeException(ex);

		}

	}

	public static void main(String[] args) {

		// System.out.println(SIZE);

		System.out.println("Start...");

		Database db = new Database();

		try {

			ArrayList al = db.top("zcarticle", "id", "type", "addtime", 200);

			System.out.println(al.size());

			System.out.println(al);

		} catch (SQLException ex) {

			ex.printStackTrace();

		}

	}

	/**
	 * 连接返回连接池或关闭
	 * 
	 * @param conn
	 *            Connection
	 */
	public void close(Connection conn) {

		close(null, null, conn);

	}

	/**
	 * 关闭PreparedStatement
	 * 
	 * @param ps
	 *            PreparedStatement
	 */
	public void close(PreparedStatement ps) {

		close(null, ps, null);

	}

	/**
	 * 关闭PreparedStatement和Connection
	 * 
	 * @param ps
	 *            PreparedStatement
	 * @param conn
	 *            Connection
	 */
	public void close(PreparedStatement ps, Connection conn) {

		close(null, ps, conn);

	}

	/**
	 * 关闭PreparedStatement和ResultSet
	 * 
	 * @param rs
	 *            ResultSet
	 * @param ps
	 *            PreparedStatement
	 */
	public void close(ResultSet rs, PreparedStatement ps) {

		close(rs, ps, null);

	}

	/**
	 * 关闭PreparedStatement、ResultSet和Connection
	 * 
	 * @param rs
	 *            ResultSet
	 * @param ps
	 *            PreparedStatement
	 * @param conn
	 *            Connection
	 */
	public void close(ResultSet rs, PreparedStatement ps, Connection conn) {

		if (rs != null) {

			try {

				rs.close();

				rs = null;

			} catch (SQLException ex) {

				ex.printStackTrace();

			}

		}

		if (ps != null) {

			try {

				ps.close();

				ps = null;

			} catch (SQLException ex) {

				ex.printStackTrace();

			}

		}

		if (conn != null) {

			if (POOL.size() < SIZE) {

				POOL.add(conn);

			} else {

				try {

					conn.close();

					conn = null;

				} catch (SQLException ex) {

					ex.printStackTrace();

				}

			}

		}

	}

	/**
	 * 执行SQL，返回count
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public int count(String sql) throws SQLException {

		int count = 0;

		if (sql == null) {

			return count;

		}

		sql = sql.trim();

		if (sql.length() == 0) {

			return count;

		}

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql);

			rs = ps.executeQuery();

			if (rs.next()) {

				count = rs.getInt(1);

			}

		} catch (SQLException ex) {

			throw ex;

		} finally {

			close(rs, ps, conn);

		}

		return count;

	}

	public int count(String table, HashMap where) throws SQLException {

		return count(table, where, true);

	}

	public int count(String table, HashMap where, boolean and) throws SQLException {

		if (table == null) {

			return 0;

		}

		table = table.trim();

		if (table.length() == 0) {

			return 0;

		}

		StringBuilder sql = new StringBuilder();

		sql.append("SELECT COUNT(*) FROM ");

		sql.append(table);

		Object[] whereKeys = null;

		if (where != null && where.size() > 0) {

			whereKeys = where.keySet().toArray();

			sql.append(" WHERE ");

			sql.append(whereKeys[0]);

			sql.append("=?");

			for (int i = 1; i < whereKeys.length; i++) {

				if (and) {

					sql.append(" AND ");

				} else {

					sql.append(" OR ");

				}

				sql.append(whereKeys[i]);

				sql.append("=?");

			}

		}

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql.toString());

			int idx = 1;

			if (whereKeys != null) {

				ps.setObject(idx++, where.get(whereKeys[0]));

				for (int i = 1; i < whereKeys.length; i++) {

					ps.setObject(idx++, where.get(whereKeys[i]));

				}

			}

			rs = ps.executeQuery();

			if (rs.next()) {

				return rs.getInt(1);

			}

		} catch (SQLException ex) {

			throw ex;

		} finally {

			sql = null;

			whereKeys = null;

			close(rs, ps, conn);

		}

		return 0;

	}

	public int count(String table, String whereKey, Object whereValue) throws SQLException {

		return count(table, whereKey, new Object[] { whereValue });

	}

	/**
	 * 按单个字段的多个值查询记录
	 * 
	 * @param table
	 * @param key
	 * @param values
	 * @return
	 * @throws SQLException
	 */
	public int count(String table, String key, Object[] values) throws SQLException {

		if (table == null) {

			return 0;

		}

		if (key != null) {

			key = key.trim();

			if (key.length() == 0) {

				key = null;

			}

		}

		if (values == null) {

			values = new Object[] { null };

		}

		StringBuilder sql = new StringBuilder();

		sql.append("SELECT COUNT(*) FROM ");

		sql.append(table);

		if (key != null && values.length > 0) {

			sql.append(" WHERE ");

			sql.append(key);

			sql.append(" IN (?");

			for (int i = 1; i < values.length; i++) {

				sql.append(",?");

			}

			sql.append(")");

		}

		// System.out.println(sql.toString());

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql.toString());

			int idx = 1;

			if (key != null && values.length > 0) {

				ps.setObject(idx++, values[0]);

				for (int i = 1; i < values.length; i++) {

					ps.setObject(idx++, values[i]);

				}

			}

			rs = ps.executeQuery();

			if (rs.next()) {

				return rs.getInt(1);

			}

		} catch (SQLException ex) {

			throw ex;

		} finally {

			sql = null;

			close(rs, ps, conn);

		}

		return 0;

	}

	public int count(StringBuilder sql) throws SQLException {

		if (sql == null) {

			return 0;

		}

		return count(sql.toString());

	}

	/**
	 * 按多个字段值删除记录
	 * 
	 * @param table
	 * @param where
	 * @throws SQLException
	 */
	public void delete(String table, HashMap where) throws SQLException {

		delete(table, where, true);

	}

	/**
	 * 按多个字段值删除记录
	 * 
	 * @param table
	 * @param where
	 * @param and
	 * @throws SQLException
	 */
	public void delete(String table, HashMap where, boolean and) throws SQLException {

		if (table == null || where == null || where.isEmpty()) {

			return;

		}

		Object[] wk = where.keySet().toArray();

		StringBuilder sql = new StringBuilder();

		sql.append("DELETE FROM ");

		sql.append(table);

		sql.append(" WHERE ");

		sql.append(wk[0]);

		sql.append("=?");

		for (int i = 1; i < wk.length; i++) {

			if (and) {
				sql.append(" AND ");
			} else {
				sql.append(" OR ");
			}

			sql.append(wk[i]);
			sql.append("=?");

		}

		Connection conn = null;

		PreparedStatement ps = null;

		try {

			conn = getConnection();

			conn.setAutoCommit(false);

			ps = conn.prepareStatement(sql.toString());

			int idx = 1;

			ps.setObject(idx++, where.get(wk[0]));

			for (int i = 1; i < wk.length; i++) {

				ps.setObject(idx++, where.get(wk[i]));

			}

			ps.execute();

			conn.commit();

		} catch (SQLException ex) {

			if (conn != null) {

				conn.rollback();

			}

			throw ex;

		} finally {

			sql = null;

			wk = null;

			if (conn != null) {

				conn.setAutoCommit(true);

			}

			close(ps, conn);

		}

	}

	/**
	 * 按单个字段值删除记录
	 * 
	 * @param table
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	public void delete(String table, String key, Object value) throws SQLException {

		if (table == null || key == null) {

			return;

		}

		delete(table, key, new Object[] { value });

	}

	/**
	 * 按单个字段多个值删除多个记录
	 * 
	 * @param table
	 * @param key
	 * @param values
	 * @throws SQLException
	 */
	public void delete(String table, String key, Object[] values) throws SQLException {

		if (table == null || key == null) {

			return;

		}

		if (values == null) {

			values = new Object[] { null };

		}

		if (values.length == 0) {

			return;

		}

		StringBuilder sql = new StringBuilder();

		sql.append("DELETE FROM ");

		sql.append(table);

		sql.append(" WHERE ");

		sql.append(key);

		sql.append(" IN (?");

		for (int i = 1; i < values.length; i++) {

			sql.append(",?");

		}

		sql.append(")");

		// System.out.println(sql);

		Connection conn = null;

		PreparedStatement ps = null;

		try {

			conn = getConnection();

			conn.setAutoCommit(false);

			ps = conn.prepareStatement(sql.toString());

			int idx = 1;

			ps.setObject(idx++, values[0]);

			for (int i = 1; i < values.length; i++) {

				ps.setObject(idx++, values[i]);

			}

			ps.execute();

			conn.commit();

		} catch (SQLException ex) {

			if (conn != null) {

				conn.rollback();

			}

			throw ex;

		} finally {

			sql = null;

			if (conn != null) {

				conn.setAutoCommit(true);

			}

			close(ps, conn);

		}

	}

	/**
	 * 执行SQL删除或更新语句
	 * 
	 * @param sql
	 * @throws SQLException
	 */
	public void execute(String sql) throws SQLException {

		if (sql == null) {

			return;

		}

		Connection conn = null;

		PreparedStatement ps = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql);

			ps.execute();

		} catch (SQLException ex) {

			throw ex;

		} finally {

			close(ps, conn);

		}

	}

	/**
	 * 执行SQL删除或更新语句
	 * 
	 * @param sql
	 * @throws SQLException
	 */
	public void execute(StringBuilder sql) throws SQLException {

		if (sql == null) {

			return;

		}

		execute(sql.toString());

	}

	/**
	 * 按多个字段值查询，返回第一个记录
	 * 
	 * @param table
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public HashMap get(String table, HashMap where) throws SQLException {

		if (table == null || where == null || where.isEmpty()) {

			return null;

		}

		ArrayList rows = select(table, where, 1);

		HashMap row = null;

		if (rows.size() > 0) {

			row = (HashMap) rows.get(0);

		}

		rows = null;

		return row;

	}

	/**
	 * 按多个字段值查询，返回第一个记录
	 * 
	 * @param table
	 * @param where
	 * @param and
	 * @return
	 * @throws SQLException
	 */
	public HashMap get(String table, HashMap where, boolean and) throws SQLException {

		if (table == null || where == null || where.isEmpty()) {

			return null;

		}

		ArrayList rows = select(table, where, and, 1);

		HashMap row = null;

		if (rows.size() > 0) {

			row = (HashMap) rows.get(0);

		}

		rows = null;

		return row;

	}

	/**
	 * 按单个字段值查询，返回第一个记录
	 * 
	 * @param table
	 * @param key
	 * @param value
	 * @return
	 * @throws SQLException
	 */
	public HashMap get(String table, String key, Object value) throws SQLException {

		if (table == null || key == null) {

			return null;

		}

		ArrayList rows = select(table, key, value, 1);

		HashMap row = null;

		if (rows.size() > 0) {

			row = (HashMap) rows.get(0);

		}

		rows = null;

		return row;

	}

	/**
	 * 获取数据库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {

		Connection conn = null;

		if (POOL.size() > 0) {

			conn = (Connection) POOL.remove(0);

			if (!conn.isValid(3)) {

				conn = null;

			}

		}

		if (conn == null) {

			conn = DriverManager.getConnection(URL, USER, PASSWORD);

		}

		return conn;

	}

	/**
	 * 增加多个记录
	 * 
	 * @param table
	 * @param rows
	 * @throws SQLException
	 */
	public void insert(String table, ArrayList rows) throws SQLException {

		if (table == null || rows == null || rows.isEmpty()) {

			return;

		}

		table = table.toLowerCase();

		Connection conn = null;

		StringBuilder sql = null;

		Object[] cols = null;

		HashMap row = null;

		try {

			conn = getConnection();

			conn.setAutoCommit(false);

			PreparedStatement ps = null;

			int size = rows.size();

			for (int s = 0; s < size; s++) {

				row = (HashMap) rows.get(s); // 行

				if (row.isEmpty()) {

					continue;

				}

				cols = row.keySet().toArray(); // 列

				sql = new StringBuilder("INSERT INTO ");

				sql.append(table);

				sql.append("(");

				sql.append(cols[0]);

				for (int i = 1; i < cols.length; i++) {

					sql.append(",");

					sql.append(cols[i]);

				}

				sql.append(") VALUES(?");

				for (int i = 1; i < cols.length; i++) {

					sql.append(",?");

				}

				sql.append(")");

				ps = conn.prepareStatement(sql.toString());

				for (int i = 0; i < cols.length; i++) {

					ps.setObject(i + 1, row.get(cols[i]));

				}

				ps.execute();

				ps.close();

			}

			conn.commit();

		} catch (SQLException ex) {

			if (conn != null) {

				conn.rollback();

			}

			throw ex;

		} finally {

			row = null;

			cols = null;

			sql = null;

			if (conn != null) {

				conn.setAutoCommit(true);

			}

			close(conn);

		}

	}

	/**
	 * 增加单个记录
	 * 
	 * @param table
	 * @param row
	 * @throws SQLException
	 */
	public void insert(String table, HashMap row) throws SQLException {

		if (table == null || row == null || row.isEmpty()) {

			return;

		}

		ArrayList rows = new ArrayList();

		rows.add(row);

		insert(table, rows);

		rows = null;

	}

	/**
	 * 获取指定列的最大值
	 * 
	 * @param table
	 *            表名
	 * @param key
	 *            列名
	 * @return
	 * @throws SQLException
	 */
	public long max(String table, String key) throws SQLException {

		if (table == null || key == null) {

			return 0L;

		}

		table = table.trim();

		key = key.trim();

		if (table.length() == 0 || key.length() == 0) {

			return 0L;

		}

		String sql = "SELECT MAX(" + key + ") FROM " + table;

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql);

			rs = ps.executeQuery();

			if (rs.next()) {

				return rs.getLong(1);

			}

		} catch (SQLException ex) {

			throw ex;

		} finally {

			sql = null;

			close(rs, ps, conn);

		}

		return 0L;

	}

	/**
	 * 分页，最后一页
	 * 
	 * @param rs
	 * @param pageSize
	 * @return
	 * @throws SQLException
	 * 
	 *             HashMap PAGE = xxx.page(star, n);
	 * 
	 *             ArrayList RS = null;
	 * 
	 *             int PC = 0, PN = 0, RC = 0, RN = 0, SIZE = 0;
	 * 
	 *             if (PAGE != null) {
	 * 
	 *             RS = (ArrayList)PAGE.get("RS");
	 * 
	 *             PC = (Integer)PAGE.get("PC");
	 * 
	 *             PN = (Integer)PAGE.get("PN");
	 * 
	 *             RC = (Integer)PAGE.get("RC");
	 * 
	 *             RN = (Integer)PAGE.get("RN");
	 * 
	 *             SIZE = RS.size();
	 * 
	 *             }
	 */
	public HashMap page(ResultSet rs, int pageSize) throws SQLException {

		if (rs == null || pageSize < 1) {

			return null;

		}

		return page(rs, pageSize, Integer.MAX_VALUE);

	}

	/**
	 * 分页，指定页码
	 * 
	 * @param rs
	 * @param pageSize
	 * @param pageNo
	 * @return
	 * @throws SQLException
	 * 
	 *             HashMap PAGE = xxx.page(star, n);
	 * 
	 *             ArrayList RS = null;
	 * 
	 *             int PC = 0, PN = 0, RC = 0, RN = 0, SIZE = 0;
	 * 
	 *             if (PAGE != null) {
	 * 
	 *             RS = (ArrayList)PAGE.get("RS");
	 * 
	 *             PC = (Integer)PAGE.get("PC");
	 * 
	 *             PN = (Integer)PAGE.get("PN");
	 * 
	 *             RC = (Integer)PAGE.get("RC");
	 * 
	 *             RN = (Integer)PAGE.get("RN");
	 * 
	 *             SIZE = RS.size();
	 * 
	 *             }
	 */
	public HashMap page(ResultSet rs, int pageSize, int pageNo) throws SQLException {

		HashMap result = null;

		if (rs == null || pageSize < 1 || pageNo < 1) {

			return result;

		}

		ArrayList rows = new ArrayList();

		String cn[] = null;

		String CN[] = null;

		ResultSetMetaData meta = null;

		try {

			rs.last();

			int rowCount = rs.getRow();

			if (rowCount == 0) {

				return result;

			}

			int mod = rowCount % pageSize;

			int pageCount = rowCount / pageSize + (mod == 0 ? 0 : 1);

			int currentPageSize = pageSize;

			if (pageNo >= pageCount) {

				pageNo = pageCount;

				if (mod > 0) {

					currentPageSize = mod;

				}

			}

			int rowNo = 1;

			if (pageNo == 1) {

				rs.beforeFirst();

			} else {

				int cursor = (pageNo - 1) * pageSize;

				rs.absolute(cursor);

				rowNo = cursor + 1;

			}

			meta = rs.getMetaData();

			int cc = meta.getColumnCount();

			cn = new String[cc];

			CN = new String[cc];

			for (int i = 0; i < cc; i++) {

				cn[i] = meta.getColumnName(i + 1);

				CN[i] = cn[i].toUpperCase();

			}

			for (int r = 0; r < currentPageSize && rs.next(); r++) {

				HashMap row = new HashMap();

				for (int i = 0; i < cc; i++) {

					Object o = rs.getObject(cn[i]);

					if (o != null) {

						row.put(CN[i], o);

					}

				}

				rows.add(row);

			}

			result = new HashMap();

			result.put("RS", rows);

			result.put("PC", pageCount);

			result.put("PN", pageNo);

			result.put("RC", rowCount);

			result.put("RN", rowNo);

		} catch (SQLException ex) {

			rows = null;

			throw ex;

		} finally {

			cn = null;

			CN = null;

			meta = null;

		}

		return result;

	}

	/**
	 * 分页，指定页码
	 * 
	 * @param rs
	 * @param pageSize
	 * @param pageNo
	 * @return
	 * @throws SQLException
	 * 
	 *             HashMap PAGE = xxx.page(star, n);
	 * 
	 *             ArrayList RS = null;
	 * 
	 *             int PC = 0, PN = 0, RC = 0, RN = 0, SIZE = 0;
	 * 
	 *             if (PAGE != null) {
	 * 
	 *             RS = (ArrayList)PAGE.get("RS");
	 * 
	 *             PC = (Integer)PAGE.get("PC");
	 * 
	 *             PN = (Integer)PAGE.get("PN");
	 * 
	 *             RC = (Integer)PAGE.get("RC");
	 * 
	 *             RN = (Integer)PAGE.get("RN");
	 * 
	 *             SIZE = RS.size();
	 * 
	 *             }
	 */
	public HashMap page(ResultSet rs, int pageSize, String pageNo) throws SQLException {

		if (rs == null || pageSize < 1) {

			return null;

		}

		if (pageNo == null) {

			return page(rs, pageSize);

		}

		try {

			return page(rs, pageSize, Integer.valueOf(pageNo));

		} catch (NumberFormatException ex) {

			return page(rs, pageSize);

		}

	}

	/**
	 * 分页，最后一页
	 * 
	 * @param sql
	 * @param pageSize
	 * @return
	 * @throws SQLException
	 * 
	 *             HashMap PAGE = xxx.page(star, n);
	 * 
	 *             ArrayList RS = null;
	 * 
	 *             int PC = 0, PN = 0, RC = 0, RN = 0, SIZE = 0;
	 * 
	 *             if (PAGE != null) {
	 * 
	 *             RS = (ArrayList)PAGE.get("RS");
	 * 
	 *             PC = (Integer)PAGE.get("PC");
	 * 
	 *             PN = (Integer)PAGE.get("PN");
	 * 
	 *             RC = (Integer)PAGE.get("RC");
	 * 
	 *             RN = (Integer)PAGE.get("RN");
	 * 
	 *             SIZE = RS.size();
	 * 
	 *             }
	 */
	public HashMap page(String sql, int pageSize) throws SQLException {

		if (sql == null || pageSize < 1) {

			return null;

		}

		return page(sql, pageSize, Integer.MAX_VALUE);

	}

	/**
	 * 分页，指定页码
	 * 
	 * @param sql
	 * @param pageSize
	 * @param pageNo
	 * @return
	 * @throws SQLException
	 * 
	 *             HashMap PAGE = xxx.page(star, n);
	 * 
	 *             ArrayList RS = null;
	 * 
	 *             int PC = 0, PN = 0, RC = 0, RN = 0, SIZE = 0;
	 * 
	 *             if (PAGE != null) {
	 * 
	 *             RS = (ArrayList)PAGE.get("RS");
	 * 
	 *             PC = (Integer)PAGE.get("PC");
	 * 
	 *             PN = (Integer)PAGE.get("PN");
	 * 
	 *             RC = (Integer)PAGE.get("RC");
	 * 
	 *             RN = (Integer)PAGE.get("RN");
	 * 
	 *             SIZE = RS.size();
	 * 
	 *             }
	 */
	public HashMap page(String sql, int pageSize, int pageNo) throws SQLException {

		if (sql == null || pageSize < 1 || pageNo < 1) {

			return null;

		}

		HashMap result = null;

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql);

			rs = ps.executeQuery();

			result = page(rs, pageSize, pageNo);

		} catch (SQLException ex) {

			result = null;

			throw ex;

		} finally {

			close(rs, ps, conn);

		}

		return result;

	}

	/**
	 * 分页，指定页码
	 * 
	 * @param sql
	 * @param pageSize
	 * @param pageNo
	 * @return
	 * @throws SQLException
	 * 
	 *             HashMap PAGE = xxx.page(star, n);
	 * 
	 *             ArrayList RS = null;
	 * 
	 *             int PC = 0, PN = 0, RC = 0, RN = 0, SIZE = 0;
	 * 
	 *             if (PAGE != null) {
	 * 
	 *             RS = (ArrayList)PAGE.get("RS");
	 * 
	 *             PC = (Integer)PAGE.get("PC");
	 * 
	 *             PN = (Integer)PAGE.get("PN");
	 * 
	 *             RC = (Integer)PAGE.get("RC");
	 * 
	 *             RN = (Integer)PAGE.get("RN");
	 * 
	 *             SIZE = RS.size();
	 * 
	 *             }
	 */
	public HashMap page(String sql, int pageSize, String pageNo) throws SQLException {

		if (sql == null || pageSize < 1) {

			return null;

		}

		if (pageNo == null) {

			return page(sql, pageSize);

		}

		try {

			return page(sql, pageSize, Integer.valueOf(pageNo));

		} catch (NumberFormatException ex) {

			return page(sql, pageSize);

		}

	}

	/**
	 * 获取表的分页数
	 * 
	 * @param table
	 * @param pageSize
	 * @return
	 * @throws SQLException
	 */
	public int pageCount(String table, int pageSize) throws SQLException {

		if (table == null || pageSize < 1) {

			return 0;

		}

		String sql = "SELECT COUNT(*) FROM " + table;

		int c = count(sql);

		int cp = c / pageSize + (c % pageSize > 0 ? 1 : 0);

		sql = null;

		return cp;

	}

	/**
	 * 获取ResultSet的记录集
	 * 
	 * @param rs
	 *            ResultSet
	 * @return ArrayList 大小为0表示没有记录，每条记录使用HashMap封装，键名为大写的字段名，键值为字段值
	 * @throws SQLException
	 */
	public ArrayList select(ResultSet rs) throws SQLException {

		ArrayList rows = new ArrayList();

		if (rs == null) {

			return rows;

		}

		String cn[] = null;

		String CN[] = null;

		ResultSetMetaData meta = null;

		try {

			meta = rs.getMetaData();

			int cc = meta.getColumnCount();

			cn = new String[cc];

			CN = new String[cc];

			for (int i = 0; i < cc; i++) {

				cn[i] = meta.getColumnName(i + 1);

				CN[i] = cn[i].toUpperCase();

			}

			while (rs.next()) {

				HashMap row = new HashMap();

				for (int i = 0; i < cc; i++) {

					Object o = rs.getObject(cn[i]);

					if (o != null) {

						row.put(CN[i], o);

					}

				}

				rows.add(row);

			}

		} catch (SQLException ex) {

			rows = null;

			throw ex;

		} finally {

			cn = null;

			CN = null;

			meta = null;

		}

		return rows;

	}

	public ArrayList select(String sql) throws SQLException {

		return select(sql, 0);

	}

	public ArrayList select(String table, HashMap where) throws SQLException {

		return select(table, where, true, null, false, 0);

	}

	public ArrayList select(String table, HashMap where, boolean and) throws SQLException {

		return select(table, where, and, null, false, 0);

	}

	public ArrayList select(String table, HashMap where, boolean and, int maxRows) throws SQLException {

		return select(table, where, and, null, false, maxRows);

	}

	public ArrayList select(String table, HashMap where, boolean and, String orderKey) throws SQLException {

		return select(table, where, and, orderKey, false, 0);

	}

	public ArrayList select(String table, HashMap where, boolean and, String orderKey, boolean orderDesc) throws SQLException {

		return select(table, where, and, orderKey, orderDesc, 0);

	}

	/**
	 * 按表名、字段名及字段值条件、条件是否为AND、排序字段名、排序是否为倒序、最大返回记录数等进行查询
	 * 
	 * @param table
	 *            表名
	 * @param where
	 *            字段名及字段值条件
	 * @param and
	 *            条件是否为AND
	 * @param orderKey
	 *            排序字段名
	 * @param desc
	 *            排序是否为倒序
	 * @param maxRows
	 *            最大返回记录数
	 * @return
	 * @throws SQLException
	 */
	public ArrayList select(String table, HashMap where, boolean and, String orderKey, boolean orderDesc, int maxRows) throws SQLException {

		ArrayList rows = new ArrayList();

		if (table == null) {

			return rows;

		}

		table = table.trim();

		if (table.length() == 0) {

			return rows;

		}

		StringBuilder sql = new StringBuilder();

		sql.append("SELECT * FROM ");

		sql.append(table);

		Object[] whereKeys = null;

		if (where != null && where.size() > 0) {

			sql.append(" WHERE ");

			whereKeys = where.keySet().toArray();

			sql.append(whereKeys[0]);

			sql.append("=?");

			for (int i = 1; i < whereKeys.length; i++) {

				if (and) {
					sql.append(" AND ");
				} else {
					sql.append(" OR ");
				}

				sql.append(whereKeys[i]);
				sql.append("=?");

			}

		}

		if (orderKey != null) {

			sql.append(" ORDER BY ");

			sql.append(orderKey.trim());

			if (orderDesc) {

				sql.append(" DESC");

			}

		}

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql.toString());

			int idx = 1;

			if (whereKeys != null) {

				ps.setObject(idx++, where.get(whereKeys[0]));

				for (int i = 1; i < whereKeys.length; i++) {

					ps.setObject(idx++, where.get(whereKeys[i]));

				}

			}

			if (maxRows > 0) {

				ps.setMaxRows(maxRows);

			}

			rs = ps.executeQuery();

			rows = select(rs);

		} catch (SQLException ex) {

			rows = null;

			throw ex;

		} finally {

			sql = null;

			whereKeys = null;

			close(rs, ps, conn);

		}

		return rows;

	}

	public ArrayList select(String table, HashMap where, boolean and, String orderKey, int maxRows) throws SQLException {

		return select(table, where, and, orderKey, false, maxRows);

	}

	public ArrayList select(String table, HashMap where, int maxRows) throws SQLException {

		return select(table, where, true, null, false, maxRows);

	}

	public ArrayList select(String table, HashMap where, String orderKey) throws SQLException {

		return select(table, where, true, orderKey, false, 0);

	}

	public ArrayList select(String table, HashMap where, String orderKey, boolean orderDesc) throws SQLException {

		return select(table, where, orderKey, orderDesc);

	}

	public ArrayList select(String table, HashMap where, String orderKey, boolean orderDesc, int maxRows) throws SQLException {

		return select(table, where, true, orderKey, orderDesc, maxRows);

	}

	public ArrayList select(String table, HashMap where, String orderKey, int maxRows) throws SQLException {

		return select(table, where, true, orderKey, false, maxRows);

	}

	/**
	 * 执行SQL查询语句
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ArrayList select(String sql, int maxRows) throws SQLException {

		ArrayList rows = new ArrayList();

		if (sql == null) {

			return rows;

		}

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql);

			if (maxRows > 0) {

				ps.setMaxRows(maxRows);

			}

			rs = ps.executeQuery();

			rows = select(rs);

		} catch (SQLException ex) {

			rows = null;

			throw ex;

		} finally {

			close(rs, ps, conn);

		}

		return rows;

	}

	public ArrayList select(String table, String whereKey, Object whereValue) throws SQLException {

		return select(table, whereKey, whereValue, null, false, 0);

	}

	public ArrayList select(String table, String whereKey, Object whereValue, int maxRows) throws SQLException {

		return select(table, whereKey, whereValue, null, false, maxRows);

	}

	public ArrayList select(String table, String whereKey, Object whereValue, String orderKey) throws SQLException {

		return select(table, whereKey, whereValue, orderKey, false, 0);

	}

	public ArrayList select(String table, String whereKey, Object whereValue, String orderKey, boolean orderDesc) throws SQLException {

		return select(table, whereKey, whereValue, orderKey, orderDesc, 0);

	}

	public ArrayList select(String table, String whereKey, Object whereValue, String orderKey, boolean orderDesc, int maxRows) throws SQLException {

		HashMap where = null;

		if (whereKey != null) {

			where = new HashMap();

			where.put(whereKey, whereValue);

		}

		return select(table, where, true, orderKey, orderDesc, maxRows);

	}

	public ArrayList select(String table, String whereKey, Object whereValue, String orderKey, int maxRows) throws SQLException {

		return select(table, whereKey, whereValue, orderKey, false, maxRows);

	}

	public ArrayList select(String table, String whereKey, Object[] whereValues) throws SQLException {

		return select(table, whereKey, whereValues, null, false, 0);

	}

	public ArrayList select(String table, String whereKey, Object[] whereValues, int maxRows) throws SQLException {

		return select(table, whereKey, whereValues, null, false, maxRows);

	}

	public ArrayList select(String table, String whereKey, Object[] whereValues, String orderKey) throws SQLException {

		return select(table, whereKey, whereValues, orderKey, false, 0);

	}

	public ArrayList select(String table, String key, Object[] values, String orderKey, boolean orderDesc) throws SQLException {

		return select(table, key, values, orderKey, orderDesc, 0);

	}

	/**
	 * 按表名、字段名及多个字段值条件、排序字段名、排序是否为倒序、最大返回记录数等进行查询
	 * 
	 * @param table
	 * @param whereKey
	 * @param whereValues
	 * @param orderKey
	 * @param orderDesc
	 * @param maxRows
	 * @return
	 * @throws SQLException
	 */
	public ArrayList select(String table, String whereKey, Object[] whereValues, String orderKey, boolean orderDesc, int maxRows) throws SQLException {

		ArrayList rows = new ArrayList();

		if (table == null) {

			return rows;

		}

		table = table.trim();

		if (table.length() == 0) {

			return rows;

		}

		if (whereValues == null) {

			whereValues = new Object[] { null };

		}

		StringBuilder sql = new StringBuilder();

		sql.append("SELECT * FROM ");

		sql.append(table);

		if (whereKey != null && whereValues.length > 0) {

			sql.append(" WHERE ");

			sql.append(whereKey);

			sql.append(" IN (?");

			for (int i = 1; i < whereValues.length; i++) {

				sql.append(",?");

			}

			sql.append(")");

		}

		if (orderKey == null) {

			if (whereKey != null && whereValues.length > 0) {

				sql.append(" ORDER BY  FIELD(");

				sql.append(whereKey);

				for (int i = 0; i < whereValues.length; i++) {

					sql.append(",?");

				}

				sql.append(")");

			}

		} else {

			sql.append(" ORDER BY ");

			sql.append(orderKey.trim());

			if (orderDesc) {

				sql.append(" DESC");

			}

		}

		// System.out.println(sql.toString());

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql.toString());

			int idx = 1;

			if (whereKey != null && whereValues.length > 0) {

				for (int i = 0; i < whereValues.length; i++) {

					ps.setObject(idx++, whereValues[i]);

				}

				if (orderKey == null) {

					for (int i = 0; i < whereValues.length; i++) {

						ps.setObject(idx++, whereValues[i]);

					}

				}

			}

			if (maxRows > 0) {

				ps.setMaxRows(maxRows);

			}

			rs = ps.executeQuery();

			rows = select(rs);

		} catch (SQLException ex) {

			rows = null;

			throw ex;

		} finally {

			sql = null;

			close(rs, ps, conn);

		}

		return rows;

	}

	public ArrayList select(String table, String whereKey, Object[] whereValues, String orderKey, int maxRows) throws SQLException {

		return select(table, whereKey, whereValues, orderKey, false, maxRows);

	}

	/**
	 * 按SQL语句查询记录
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ArrayList select(StringBuilder sql) throws SQLException {

		return select(sql, 0);

	}

	/**
	 * 按SQL语句查询记录
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ArrayList select(StringBuilder sql, int maxRows) throws SQLException {

		if (sql == null) {

			return new ArrayList();

		}

		return select(sql.toString(), maxRows);

	}

	public Object[] selectArray(String sql) throws SQLException {

		return selectArray(sql, null, 0);

	}

	public Object[] selectArray(String sql, String arrayKey) throws SQLException {

		return selectArray(sql, arrayKey, 0);

	}

	/**
	 * 获取指定查询结果数组
	 * 
	 * @param sql
	 * @param arrayKey
	 * @param maxRows
	 * @return
	 * @throws SQLException
	 */
	public Object[] selectArray(String sql, String arrayKey, int maxRows) throws SQLException {

		if (sql == null) {

			return new Object[0];

		}

		sql = sql.trim();

		if (sql.length() == 0) {

			return new Object[0];

		}

		if (arrayKey != null) {

			arrayKey = arrayKey.trim();

			if (arrayKey.length() == 0) {

				arrayKey = null;

			}

		}

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		ArrayList list = new ArrayList();

		Object o = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql);

			if (maxRows > 0) {

				ps.setMaxRows(maxRows);

			}

			rs = ps.executeQuery();

			if (arrayKey == null) {

				while (rs.next()) {

					o = rs.getObject(1);

					list.add(o);

				}

			} else {

				while (rs.next()) {

					o = rs.getObject(arrayKey);

					list.add(o);

				}

			}

		} catch (SQLException ex) {

			list = null;

			throw ex;

		} finally {

			close(rs, ps, conn);

		}

		return list.toArray();

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey) throws SQLException {

		if (max) {

			return top(table, primaryKey, max, groupKey, null, null, true, 0);

		}

		return top(table, primaryKey, max, groupKey, null, null, false, 0);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, boolean orderDesc) throws SQLException {

		return top(table, primaryKey, max, groupKey, null, null, orderDesc, 0);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, boolean orderDesc, int maxRows) throws SQLException {

		return top(table, primaryKey, max, groupKey, null, null, orderDesc, maxRows);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, int maxRows) throws SQLException {

		if (max) {

			return top(table, primaryKey, max, groupKey, null, null, true, maxRows);

		}

		return top(table, primaryKey, max, groupKey, null, null, false, maxRows);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, Object[] groupValues) throws SQLException {

		if (max) {

			return top(table, primaryKey, max, groupKey, groupValues, null, true, 0);

		}

		return top(table, primaryKey, max, groupKey, groupValues, null, false, 0);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, Object[] groupValues, boolean orderDesc) throws SQLException {

		return top(table, primaryKey, max, groupKey, groupValues, null, orderDesc, 0);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, Object[] groupValues, boolean orderDesc, int maxRows) throws SQLException {

		return top(table, primaryKey, max, groupKey, groupValues, null, orderDesc, maxRows);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, Object[] groupValues, int maxRows) throws SQLException {

		if (max) {

			return top(table, primaryKey, max, groupKey, groupValues, null, true, maxRows);

		}

		return top(table, primaryKey, max, groupKey, groupValues, null, false, maxRows);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, Object[] groupValues, String topKey) throws SQLException {

		if (max) {

			return top(table, primaryKey, max, groupKey, groupValues, topKey, true, 0);

		}

		return top(table, primaryKey, max, groupKey, groupValues, topKey, false, 0);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, Object[] groupValues, String topKey, boolean orderDesc) throws SQLException {

		return top(table, primaryKey, max, groupKey, groupValues, topKey, orderDesc, 0);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, Object[] groupValues, String topKey, boolean orderDesc, int maxRows) throws SQLException {

		if (table == null || primaryKey == null || groupKey == null) {

			return new ArrayList();

		}

		table = table.trim();

		primaryKey = primaryKey.trim();

		groupKey = groupKey.trim();

		if (table.length() == 0 || primaryKey.length() == 0 || groupKey.length() == 0) {

			return new ArrayList();

		}

		String topKey_ = topKey == null ? primaryKey : topKey;

		String sql = "SELECT";

		if (max) {

			sql += " MAX";

		} else {

			sql += " MIN";

		}

		sql += "(" + topKey_ + ") AS _" + topKey_ + "  FROM " + table;

		if (groupValues != null && groupValues.length > 0) {

			sql += " WHERE " + groupKey + " IN(?";

			for (int i = 1; i < groupValues.length; i++) {

				sql += ",?";

			}

			sql += ")";

		}

		sql += " GROUP BY " + groupKey + " ORDER BY _" + topKey_;

		if (max) {

			sql += " DESC";

		}

		Connection conn = null;

		PreparedStatement ps = null;

		ResultSet rs = null;

		ArrayList list = new ArrayList();

		Object o = null;

		try {

			conn = getConnection();

			ps = conn.prepareStatement(sql);

			int p = 1;

			if (groupValues != null && groupValues.length > 0) {

				ps.setObject(p++, groupValues[0]);

				for (int i = 1; i < groupValues.length; i++) {

					ps.setObject(p++, groupValues[i]);

				}

				sql += ")";

			}

			if (maxRows > 0) {

				ps.setMaxRows(maxRows);

			}

			rs = ps.executeQuery();

			while (rs.next()) {

				o = rs.getObject(1);

				list.add(o);

			}

		} catch (SQLException ex) {

			list = null;

			throw ex;

		} finally {

			sql = null;

			close(rs, ps, conn);

		}

		Object[] pk = list.toArray();

		list = null;

		if (pk.length == 0) {

			return new ArrayList();

		}

		if (topKey == null) {

			return select(table, primaryKey, pk, topKey_, orderDesc, maxRows);

		}

		String sql2 = "SELECT";

		if (max) {

			sql2 += " MAX";

		} else {

			sql2 += " MIN";

		}

		sql2 += "(" + primaryKey + ") FROM " + table;

		sql2 += " WHERE " + topKey + " IN(?";

		for (int i = 1; i < pk.length; i++) {

			sql2 += ",?";

		}

		sql2 += ")";

		sql2 += " GROUP BY " + groupKey;

		Connection conn2 = null;

		PreparedStatement ps2 = null;

		ResultSet rs2 = null;

		ArrayList list2 = new ArrayList();

		Object o2 = null;

		try {

			conn2 = getConnection();

			ps2 = conn2.prepareStatement(sql2);

			int p2 = 1;

			ps2.setObject(p2++, pk[0]);

			for (int i = 1; i < pk.length; i++) {

				ps2.setObject(p2++, pk[i]);

			}

			sql2 += ")";

			rs2 = ps2.executeQuery();

			while (rs2.next()) {

				o2 = rs2.getObject(1);

				list2.add(o2);

			}

		} catch (SQLException ex) {

			list2 = null;

			throw ex;

		} finally {

			sql2 = null;

			pk = null;

			close(rs2, ps2, conn2);

		}

		return select(table, primaryKey, list2.toArray(), topKey_, orderDesc, maxRows);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, Object[] groupValues, String topKey, int maxRows) throws SQLException {

		if (max) {

			return top(table, primaryKey, max, groupKey, groupValues, topKey, true, maxRows);

		}

		return top(table, primaryKey, max, groupKey, groupValues, topKey, false, maxRows);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, String topKey) throws SQLException {

		if (max) {

			return top(table, primaryKey, max, groupKey, null, topKey, true, 0);

		}

		return top(table, primaryKey, max, groupKey, null, topKey, false, 0);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, String topKey, boolean orderDesc) throws SQLException {

		return top(table, primaryKey, max, groupKey, null, topKey, orderDesc, 0);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, String topKey, boolean orderDesc, int maxRows) throws SQLException {

		return top(table, primaryKey, max, groupKey, null, topKey, orderDesc, maxRows);

	}

	public ArrayList top(String table, String primaryKey, boolean max, String groupKey, String topKey, int maxRows) throws SQLException {

		if (max) {

			return top(table, primaryKey, max, groupKey, null, topKey, true, maxRows);

		}

		return top(table, primaryKey, max, groupKey, null, topKey, false, maxRows);

	}

	public ArrayList top(String table, String primaryKey, String groupKey) throws SQLException {

		return top(table, primaryKey, true, groupKey, null, null, true, 0);

	}

	public ArrayList top(String table, String primaryKey, String groupKey, int maxRows) throws SQLException {

		return top(table, primaryKey, true, groupKey, null, null, true, maxRows);

	}

	public ArrayList top(String table, String primaryKey, String groupKey, Object[] groupValues) throws SQLException {

		return top(table, primaryKey, true, groupKey, groupValues, null, true, 0);

	}

	public ArrayList top(String table, String primaryKey, String groupKey, Object[] groupValues, int maxRows) throws SQLException {

		return top(table, primaryKey, true, groupKey, groupValues, null, true, maxRows);

	}

	public ArrayList top(String table, String primaryKey, String groupKey, Object[] groupValues, String topKey) throws SQLException {

		return top(table, primaryKey, true, groupKey, groupValues, topKey, true, 0);

	}

	public ArrayList top(String table, String primaryKey, String groupKey, Object[] groupValues, String topKey, int maxRows) throws SQLException {

		return top(table, primaryKey, true, groupKey, groupValues, topKey, true, maxRows);

	}

	public ArrayList top(String table, String primaryKey, String groupKey, String topKey) throws SQLException {

		return top(table, primaryKey, true, groupKey, null, topKey, true, 0);

	}

	public ArrayList top(String table, String primaryKey, String groupKey, String topKey, int maxRows) throws SQLException {

		return top(table, primaryKey, true, groupKey, null, topKey, true, maxRows);

	}

	public void update(String table, HashMap set) throws SQLException {

		update(table, set, null, true);

	}

	public void update(String table, HashMap set, HashMap where) throws SQLException {

		update(table, set, where, true);

	}

	/**
	 * 按多个字段值更新多个字段值
	 * 
	 * @param table
	 * @param where
	 * @param set
	 * @param and
	 * @throws SQLException
	 */
	public void update(String table, HashMap set, HashMap where, boolean and) throws SQLException {

		if (table == null || set == null || set.isEmpty()) {

			return;

		}

		table = table.trim();

		if (table.length() == 0) {

			return;

		}

		StringBuilder sql = new StringBuilder();

		sql.append("UPDATE ");

		sql.append(table);

		sql.append(" SET ");

		Object[] sk = set.keySet().toArray();

		sql.append(sk[0]);

		sql.append("=?");

		for (int i = 1; i < sk.length; i++) {

			sql.append(", ");

			sql.append(sk[i]);

			sql.append("=?");

		}

		Object[] wk = null;

		if (where != null && where.size() > 0) {

			sql.append(" WHERE ");

			wk = where.keySet().toArray();

			sql.append(wk[0]);

			sql.append("=?");

			for (int i = 1; i < wk.length; i++) {

				if (and) {

					sql.append(" AND ");

				} else {

					sql.append(" OR ");

				}

				sql.append(wk[i]);

				sql.append("=?");

			}

		}

		// System.out.println(sql.toString());

		Connection conn = null;

		PreparedStatement ps = null;

		try {

			conn = getConnection();

			conn.setAutoCommit(false);

			ps = conn.prepareStatement(sql.toString());

			int idx = 1;

			for (int i = 0; i < sk.length; i++) {

				ps.setObject(idx++, set.get(sk[i]));

			}

			if (wk != null) {

				for (int i = 0; i < wk.length; i++) {

					ps.setObject(idx++, where.get(wk[i]));

				}

			}

			ps.execute();

			conn.commit();

		} catch (SQLException ex) {

			if (conn != null) {

				conn.rollback();

			}

			throw ex;

		} finally {

			sql = null;

			wk = null;

			sk = null;

			if (conn != null) {

				conn.setAutoCommit(true);

			}

			close(ps, conn);

		}

	}

	public void update(String table, HashMap set, String whereKey, Object whereValue) throws SQLException {

		Object[] whereValues = null;

		if (whereKey != null) {

			whereValues = new Object[1];

			whereValues[0] = whereValue;

		}

		update(table, set, whereKey, whereValues);

		whereValues = null;

	}

	/**
	 * 按单个字段多个值更新多个记录多个字段值
	 * 
	 * @param table
	 * @param set
	 * @param whereKey
	 * @param whereValues
	 * @throws SQLException
	 */
	public void update(String table, HashMap set, String whereKey, Object[] whereValues) throws SQLException {

		if (table == null || set == null || set.isEmpty()) {

			return;

		}

		table = table.trim();

		if (table.length() == 0) {

			return;

		}

		if (whereValues == null) {

			whereValues = new Object[] { null };

		}

		StringBuilder sql = new StringBuilder();

		sql.append("UPDATE ");

		sql.append(table);

		sql.append(" SET ");

		Object[] sk = set.keySet().toArray();

		sql.append(sk[0]);

		sql.append("=?");

		for (int i = 1; i < sk.length; i++) {

			sql.append(",");

			sql.append(sk[i]);

			sql.append("=?");

		}

		if (whereKey != null && whereValues != null && whereValues.length > 0) {

			sql.append(" WHERE ");

			sql.append(whereKey);

			sql.append(" IN (?");

			for (int i = 1; i < whereValues.length; i++) {

				sql.append(",?");

			}

			sql.append(")");

		}

		// System.out.println(sql);

		Connection conn = null;

		PreparedStatement ps = null;

		try {

			conn = getConnection();

			conn.setAutoCommit(false);

			ps = conn.prepareStatement(sql.toString());

			int idx = 1;

			for (int i = 0; i < sk.length; i++) {

				ps.setObject(idx++, set.get(sk[i]));

			}

			if (whereKey != null && whereValues != null && whereValues.length > 0) {

				for (int i = 0; i < whereValues.length; i++) {

					ps.setObject(idx++, whereValues[i]);

				}

			}

			ps.execute();

			conn.commit();

		} catch (SQLException ex) {

			if (conn != null) {

				conn.rollback();

			}

			throw ex;

		} finally {

			sql = null;

			sk = null;

			if (conn != null) {

				conn.setAutoCommit(true);

			}

			close(ps, conn);

		}

	}

	public void update(String table, String setKey, Object setValue) throws SQLException {

		update(table, setKey, setValue, null, true);

	}

	public void update(String table, String setKey, Object setValue, HashMap where) throws SQLException {

		update(table, setKey, setValue, where, true);

	}

	public void update(String table, String setKey, Object setValue, HashMap where, boolean and) throws SQLException {

		if (table == null || setKey == null) {

			return;

		}

		table = table.trim();

		setKey = setKey.trim();

		if (table.length() == 0 || setKey.length() == 0) {

			return;

		}

		HashMap set = new HashMap();

		set.put(setKey, setValue);

		update(table, set, where, and);

		set = null;

	}

	/**
	 * 按单个字段值更新记录
	 * 
	 * @param table
	 * @param whereKey
	 * @param whereValue
	 * @param setKey
	 * @param setValue
	 * @throws SQLException
	 */

	public void update(String table, String setKey, Object setValue, String whereKey, Object whereValue) throws SQLException {

		HashMap where = null;

		if (whereKey != null) {

			where = new HashMap();

			where.put(whereKey, whereValue);

		}

		update(table, setKey, setValue, where, true);

		where = null;

	}

	/**
	 * 按单个字段的多个值更新记录
	 * 
	 * @param table
	 * @param setKey
	 * @param setValue
	 * @param whereKey
	 * @param whereValues
	 * @throws SQLException
	 */
	public void update(String table, String setKey, Object setValue, String whereKey, Object[] whereValues) throws SQLException {

		if (table == null || setKey == null) {

			return;

		}

		table = table.trim();

		setKey = setKey.trim();

		if (table.length() == 0 || setKey.length() == 0) {

			return;

		}

		HashMap set = new HashMap();

		set.put(setKey, setValue);

		update(table, set, whereKey, whereValues);

		set = null;

	}

}
