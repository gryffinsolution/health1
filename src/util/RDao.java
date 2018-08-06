package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RDao {
	private static final Logger LOG = LogManager.getLogger(RDao.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public Connection getConnection(String dbURL, String user, String password) {
		LOG.info("DB_URL=" + dbURL);
		Connection con = null;
		try {
			if (dbURL.startsWith("jdbc:postgresql:")) {
				Class.forName("org.postgresql.Driver");
			} else if (dbURL.startsWith("jdbc:oracle:")) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			}
		} catch (ClassNotFoundException e) {
			LOG.error("DB Driver loading error!");
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(dbURL, user, password);
		} catch (SQLException e) {
			LOG.error("getConn Exception)");
			e.printStackTrace();
		}
		return con;
	}

	public void disconnect(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.error("disConn Exception)");
			printSQLException(e);
		}
	}

	public void insertAlert(Connection conn, String host, String oldStatus,
			String newStatus, String eventCode) {
		PreparedStatement pstInsert = null;
		try {
			conn.setAutoCommit(false);
			LOG.info("ALERT " + host + "= old:" + oldStatus + " -> new:"
					+ newStatus);
			String insertSQL = "INSERT INTO HOST_EVENT "
					+ "(HOSTNAME,ARRIVAL_TIME,LOCAL_ID,EVENT_CODE,SEVERITY,MESSAGE,START_TIME,"
					+ "LAST_EVENT_TIME,REPEAT_CNT) " + "VALUES ('" + host
					+ "',CURRENT_TIMESTAMP,0,'" + eventCode
					+ "','ERROR','status (" + oldStatus + " -> " + newStatus
					+ ")',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0)";
			LOG.trace(insertSQL);
			pstInsert = conn.prepareStatement(insertSQL);
			pstInsert.executeUpdate();
			pstInsert.close();
			conn.commit();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error(e);
		} finally {
			try {
				if (pstInsert != null)
					pstInsert.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pstInsert = null;
		}
	}

	public int getTotalHostNo(Connection con) {
		Statement stmt;
		int NoOfHost = 0;
		try {
			String sql = "SELECT DISTINCT MAX(HOST_NO) FROM HOST_INFOS ";
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				NoOfHost = rs.getInt(1);
				LOG.info("Total hosts=" + NoOfHost);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return NoOfHost;
	}

	public ArrayList<String> getHostsTest(Connection conR) {
		ArrayList<String> host = new ArrayList<String>();
		host.add("localhost.localdomain");
		return host;
	}

	public ArrayList<String> getHostsMTCustom(Connection con, int seq,
			int Total, HashMap<String, String> hostKVstatus,
			ArrayList<String> customSkipList, String sql, String skipKeyword,
			String skipColumn) {
		Statement stmt;
		ArrayList<String> hostList = new ArrayList<String>();
		try {
			int NoOfHost = getTotalHostNo(con);
			int sliceTerm = (int) Math.ceil(NoOfHost / (Total * 1.0));
			LOG.info("GAP=>" + sliceTerm);
			int sliceStart = 0;
			int sliceEnd = 0;
			sliceStart = sliceStart + sliceTerm * seq;
			sliceEnd = sliceStart + sliceTerm - 1;
			LOG.info(seq + 1 + ":" + sliceStart + "~" + sliceEnd);
			sql = sql + " AND HOST_NO > " + sliceStart + " AND HOST_NO < "
					+ sliceEnd;

			LOG.info("sql=" + sql);
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String host = rs.getString("HOSTNAME");
				LOG.info(seq + 1 + ":hostname " + host);
				hostList.add(host);
				hostKVstatus.put(host, rs.getString("STATUS"));
				String cate2 = rs.getString("EXTERNAL_OS_INFO");
				if (cate2 != null && cate2.matches(skipKeyword)) {
					customSkipList.add(host);
				}
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return hostList;
	}

	public void updateStatus(Connection con, String host, String status,
			boolean flgSysdate) {
		PreparedStatement pst = null;
		try {
			con.setAutoCommit(false);
			String sql = null;
			if (flgSysdate) {
				sql = "UPDATE HOST_INFOS SET STATUS ='" + status
						+ "',HEALTH_LAST_TIME=SYSDATE WHERE HOSTNAME='" + host
						+ "'";
			} else {
				sql = "UPDATE HOST_INFOS SET STATUS ='" + status
						+ "' WHERE HOSTNAME='" + host + "'";
			}
			LOG.trace(sql);
			pst = con.prepareStatement(sql);
			pst.executeUpdate();
			con.commit();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}

	public void setWorkingTimestamp(Connection conR, String dbURL, int thNo) {
		PreparedStatement pst = null;
		try {
			conR.setAutoCommit(false);

			String sqlLastUpdateTime = null;

			if (dbURL.startsWith("jdbc:postgresql:")) {
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=NOW() WHERE SERVICE_NAME='HostUpChecker"
						+ thNo + "'"; // pgsql
			} else if (dbURL.startsWith("jdbc:oracle:")) {
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=SYSDATE WHERE SERVICE_NAME='HostUpChecker"
						+ thNo + "'";// oracle
			} else {
				LOG.fatal("Can't find right JDBC. please check you config.xml");
				System.exit(0);
			}

			LOG.trace(sqlLastUpdateTime);
			pst = conR.prepareStatement(sqlLastUpdateTime);
			pst.executeUpdate();
			conR.commit();
			pst.close();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {

		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}
}