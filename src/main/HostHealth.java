package main;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import util.Conf;

//

public class HostHealth {
	private static final Logger LOG = LogManager.getLogger(HostHealth.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public static void main(String[] args) {

		Conf cf = new Conf();
		if (args.length != 0 && args[0] != null) {
			cf.setConfFile(args[0]);
		} else {
			LOG.error("there is no config.xml as a args[0]");
			System.exit(0);
		}
		//
		//
		//
		//
		String rdbUrl = cf.getDbURL();
		String rdbUser = cf.getSingleString("user");
		String rdbPasswd = cf.getSingleString("password");
		int thAll = cf.getSinglefValue("no_of_thread");
		int agentPort = cf.getSinglefValue("agent_port");
		int customPort = cf.getSinglefValue("custom_port");
		String customServiceName = cf.getSingleString("custom_service_name");
		String sql = cf.getSingleString("get_host_sql");
		String skipKeyword = cf.getSingleString("custom_skip_keyword");
		String skipColumn = cf.getSingleString("custom_skip_column");
		int agentTimeout = cf.getSinglefValue("agent_delay_timeout_second");

		if (customServiceName == null) {
			LOG.error("customServiceName is empty");
			System.exit(0);
		}

		ExecutorService pool = Executors.newFixedThreadPool(thAll);
		Set<Future<Boolean>> set = new HashSet<Future<Boolean>>();

		for (int thNo = 1; thNo <= thAll; thNo++) {
			Callable callable = new Worker(thNo, thAll, rdbUrl, rdbUser,
					rdbPasswd, agentPort, customPort, customServiceName, sql,
					skipKeyword, skipColumn, agentTimeout);
			Future future = pool.submit(callable);
			set.add(future);
		}
		pool.shutdown();
	}
}
