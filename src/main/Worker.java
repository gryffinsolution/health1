package main;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import util.RDao;
import util.Sock;

public class Worker implements Callable<Boolean> {
	private static final Logger LOG = LogManager.getLogger(Worker.class);
	int thNo;
	int thAll;
	String rdbUrl;
	String rdbUser;
	String rdbPassword;
	int agentPort;
	int customPort;
	String customServiceName;
	String sql;
	String skipKeyword;
	String skipColumn;

	public Worker(int thNo, int thAll, String rdbUrl, String rdbUser,
			String rdbPasswd, int agentPort, int customPort,
			String customServiceName, String sql, String skipKeyword,
			String skipColumn) {
		this.thNo = thNo;
		this.thAll = thAll;
		this.rdbUrl = rdbUrl;
		this.rdbUser = rdbUser;
		this.rdbPassword = rdbPasswd;
		this.agentPort = agentPort;
		this.customPort = customPort;
		this.customServiceName = customServiceName;
		this.sql = sql;
		this.skipKeyword = skipKeyword;
		this.skipColumn = skipColumn;
	}

	@Override
	public Boolean call() throws Exception {
		RDao rDao = new RDao();
		Connection conn = rDao.getConnection(rdbUrl, rdbUser, rdbPassword);
		HashMap<String, String> hostKVstatus = new HashMap<String, String>();
		ArrayList<String> nonCustomCheckServerList = new ArrayList<String>();
		ArrayList<String> hosts = rDao.getHostsMTCustom(conn, thNo - 1, thAll,
				hostKVstatus, nonCustomCheckServerList, sql, skipKeyword,
				skipColumn);
		int i = 0;
		Sock sock = new Sock();
		DateTime start = new DateTime();
		for (String host : hosts) {
			LOG.trace(thNo + "-" + i + ":Checking:" + host);
			i++;
			boolean bAgent = sock.isPortWorking(host, agentPort);
			boolean bCustom = sock.isPortWorking(host, customPort);

			if (nonCustomCheckServerList.contains(host)) {
				bCustom = true;
				LOG.info(host + " was custom port skipped.");
			}

			boolean bPing = sock.isPingWorking(host);
			boolean flgSysdate = true;
			String status = "unknown";
			if (!bAgent && !bCustom && !bPing) {
				String oldStatus = hostKVstatus.get(host);
				status = "abnormalT";
				if (oldStatus != null && oldStatus.matches("abnormal")) {
					status = "abnormal";
				}
			}
			if (!bAgent && !bCustom && bPing) {
				status = "abnormal_agent_" + customServiceName + "T";
				String oldStatus = hostKVstatus.get(host);
				if (oldStatus != null
						&& oldStatus.matches("abnormal_agent_"
								+ customServiceName)) {
					status = "abnormal_agent_" + customServiceName;
				}
			}
			if (!bAgent && bCustom && !bPing)
				status = "abnormal_agent_ping";
			if (!bAgent && bCustom && bPing)
				status = "abnormal_agent";
			if (bAgent && !bCustom && !bPing)
				status = "abnormal_" + customServiceName + "_ping";
			if (bAgent && !bCustom && bPing) {
				status = "abnormal_" + customServiceName + "T";
				String oldStatus = hostKVstatus.get(host);
				if (oldStatus != null
						&& oldStatus.matches("abnormal_" + customServiceName)) {
					status = "abnormal_" + customServiceName;
				}
			}
			if (bAgent && bCustom && !bPing)
				status = "abnormal_ping";
			if (bAgent && bCustom && bPing) {
				status = "up";
				String oldStatus = hostKVstatus.get(host);
				if (oldStatus != null) {
					if (!oldStatus.matches("up")) {
						if (oldStatus.matches("abnormal_" + customServiceName)
								|| oldStatus.matches("abnormal_"
										+ customServiceName + "_agent")
								|| oldStatus.matches("abnormal")) {
							rDao.insertAlert(conn, host, oldStatus, status,
									"UP000"); // critical->up
						} else {
							rDao.insertAlert(conn, host, oldStatus, status,
									"UP001"); // non-critical->up
						}
					}
				}
			}
			LOG.info(host + ":bAgent=" + bAgent + " ,:bCustom=" + bCustom
					+ ",:bPing=" + bPing);
			rDao.updateStatus(conn, host, status, flgSysdate);
		}
		rDao.setWorkingTimestamp(conn, rdbUrl, thNo);
		DateTime end = new DateTime();
		Duration elapsedTime = new Duration(start, end);
		LOG.info(elapsedTime);
		rDao.disconnect(conn);
		return true;
	}
}