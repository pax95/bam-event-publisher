package dk.dr.drip.event.publisher;

import java.io.File;
import java.net.MalformedURLException;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.AuthenticationException;
import org.wso2.carbon.databridge.commons.exception.NoStreamDefinitionExistException;
import org.wso2.carbon.databridge.commons.exception.StreamDefinitionException;
import org.wso2.carbon.databridge.commons.exception.TransportException;

public class BamPublisher {
	private boolean loggedIn = false;
	private String host;
	private String userName;
	private String password;
	private String streamId;
	private DataPublisher dataPublisher;
	private final static String STREAM_NAME = "adapter_service_data_publisher";
	private final static String STREAM_VERSION = "1.0.0";
	private final static Logger log = LoggerFactory.getLogger(BamPublisher.class);

	public void publish(String adapter, long timeStamp, String messageId, long payloadSize) {
		try {
			connectIfNecessary();
			Object[] payload = DataBuilder.buildPayloadArray(adapter, messageId, timeStamp, payloadSize);
			dataPublisher.publish(streamId, timeStamp, new Object[] { adapter }, null, payload);
		} catch (Exception e) {
			loggedIn = false;
			throw new RuntimeException(e);
		}
	}

	private void connectIfNecessary() {
		if (!loggedIn) {
			log.debug("Not connected/logged in, connecting to: {}", host);
			loggedIn = connect();
			if (loggedIn) {
				log.info("Connected and logged in to: " + host);
			}
		}
	}

	private boolean connect() {
		boolean connected = false;
		int attempt = 0;
		setTrustStoreParams();
		while (!connected) {
			try {
				if (log.isTraceEnabled() && attempt > 0) {
					log.trace("Reconnect attempt #{} connecting to {}", attempt, host);
				}
				dataPublisher = new DataPublisher("tcp://" + host, userName, password);
				streamId = dataPublisher.findStream(STREAM_NAME, STREAM_VERSION);
				connected = true;
			} catch (MalformedURLException e) {
				throw new RuntimeException(e);
			} catch (AgentException e) {
				throw new RuntimeException("Unable to connect to host " + host, e);
			} catch (AuthenticationException e) {
				throw new RuntimeException("Unable to authenticate.", e);
			} catch (TransportException e) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
				}
			} catch (StreamDefinitionException e) {
				throw new RuntimeException("Unknown streamDefinition " + STREAM_NAME + " vers. " + STREAM_VERSION, e);
			} catch (NoStreamDefinitionExistException e) {
				throw new RuntimeException("Unknown streamDefinition " + STREAM_NAME + " vers. " + STREAM_VERSION, e);
			}
			attempt++;
			if (attempt > 5) {
				throw new RuntimeException("Unable to connect to " + host);
			}
		}
		return true;
	}

	private void setTrustStoreParams() {
		File filePath = new File("src/main/resources");
		if (!filePath.exists()) {
			filePath = new File("resources");
		}
		String trustStore = filePath.getAbsolutePath();
		System.setProperty("javax.net.ssl.trustStore", trustStore + "/client-truststore.jks");
		System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

	}

	@PreDestroy
	public void disconnect() {
		if (dataPublisher != null && loggedIn) {
			dataPublisher.stop();
		}
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}
