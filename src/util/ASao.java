package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ASao {
	private static final Logger LOG = LogManager.getLogger(ASao.class);

	public boolean isWorking(String host, int port, int timeout) {
		String url = "http://" + host + ":" + port + "/getstatus";
		HttpURLConnection con = null;
		try {
			URL myurl = new URL(url);
			con = (HttpURLConnection) myurl.openConnection();
			con.setRequestMethod("GET");
			con.setReadTimeout(1000);
			con.setConnectTimeout(1000);
			StringBuilder content;
			try (BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()))) {

				String line;
				content = new StringBuilder();

				while ((line = in.readLine()) != null) {
					content.append(line);
					content.append(System.lineSeparator());
				}
			}
			LOG.info(content.toString());
			con.disconnect();
			String retStr = content.toString();
			retStr = retStr.trim();
			long agentEpTime = Long.parseLong(retStr);
			long svrEpTime = Instant.now().toEpochMilli() / 1000L;
			LOG.info("agent:svrTime " + agentEpTime + ":" + svrEpTime);
			if (agentEpTime > svrEpTime - timeout) {
				return true;
			} else {
				return false;
			}
		} catch (MalformedURLException e) {
			LOG.error(e);
			return false;
		} catch (ProtocolException e) {
			LOG.error(e);
			return false;
		} catch (SocketTimeoutException e) {
			LOG.error(host + " " + e); // TODO
			return false;
		} catch (IOException e) {
			LOG.error(e);
			return false;
		} catch (Exception e) {
			LOG.error(e);
			return false;
		} finally {
			con.disconnect();
		}
	}
}