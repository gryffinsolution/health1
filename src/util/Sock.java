package util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Sock {
	private static final Logger LOG = LogManager.getLogger(Sock.class);

	public boolean isPingWorking(String host) {
		String strRet = null;
		try {
			InetAddress targetIp = InetAddress.getByName(host);
			boolean reachable = targetIp.isReachable(500);
			LOG.info(host + "(" + targetIp + "):ping " + reachable);
			return reachable;
		} catch (SocketTimeoutException e) {
			strRet = "timeout";
			LOG.error(host + " ping " + strRet);
			return false;
		} catch (UnknownHostException e) {
			strRet = "unknowHost";
			LOG.error(host + " ping " + strRet);
			return false;
		} catch (ConnectException e) {
			strRet = "noConn";
			LOG.error(host + " ping " + strRet);
			return false;
		} catch (IOException e) {
			strRet = "IOError";
			LOG.error(host + " ping " + strRet);
			return false;
		}
	}

	public boolean isPortWorking(String host, int port) {
		String strRet = null;
		try {
			Socket socket = new Socket();
			SocketAddress addr = new InetSocketAddress(host, port);
			socket.connect(addr, 500); // sec
			socket.setSoTimeout(500);
			InputStream in = socket.getInputStream();
			DataInputStream din = new DataInputStream(in);
			OutputStream out = socket.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			dos.close();
			out.close();
			din.close();
			socket.close();
			strRet = "up";
			LOG.info(host + ":" + port + "=" + strRet);
			return true;
		} catch (SocketTimeoutException e) {
			strRet = "timeout";
			LOG.error(host + ":" + port + " " + strRet);
			return false;
		} catch (UnknownHostException e) {
			strRet = "unknowHost";
			LOG.error(host + ":" + port + " " + strRet);
			return false;
		} catch (ConnectException e) {
			strRet = "noConn";
			LOG.error(host + ":" + port + " " + strRet);
			return false;
		} catch (IOException e) {
			strRet = "IOError";
			LOG.error(host + ":" + port + " " + strRet);
			return false;
		}
	}
}
