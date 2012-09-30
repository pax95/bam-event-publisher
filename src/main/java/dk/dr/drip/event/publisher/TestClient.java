/*
 *  Copyright (c) 2005-2012, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package dk.dr.drip.event.publisher;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.AuthenticationException;
import org.wso2.carbon.databridge.commons.exception.DifferentStreamDefinitionAlreadyDefinedException;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.carbon.databridge.commons.exception.NoStreamDefinitionExistException;
import org.wso2.carbon.databridge.commons.exception.StreamDefinitionException;
import org.wso2.carbon.databridge.commons.exception.TransportException;
import org.wso2.carbon.databridge.commons.exception.UndefinedEventTypeException;

public class TestClient {
	Logger log = LoggerFactory.getLogger(TestClient.class);

	public static void main(String[] args) throws UndefinedEventTypeException, AgentException, MalformedURLException,
			AuthenticationException, MalformedStreamDefinitionException, StreamDefinitionException, TransportException,
			InterruptedException, DifferentStreamDefinitionAlreadyDefinedException, NoStreamDefinitionExistException {

		TestClient testClient = new TestClient();
		testClient.testSendingEvent();
	}

	public void testSendingEvent() throws MalformedURLException, AuthenticationException, TransportException,
			AgentException, UndefinedEventTypeException, DifferentStreamDefinitionAlreadyDefinedException,
			InterruptedException, MalformedStreamDefinitionException, StreamDefinitionException,
			NoStreamDefinitionExistException {

		setTrustStoreParams();
		Thread.sleep(2000);

		// according to the convention the authentication port will be 7611+100=
		// 7711 and its host will be the same
		DataPublisher dataPublisher = new DataPublisher("tcp://localhost:7611", "admin", "admin");
		String stream = dataPublisher.findStream("adapter_service_data_publisher", "1.0.0");

		log.info("1st stream defined: " + stream);
		String adapter = "dalet-nnp-in";
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, -1);
		for (int i = 0; i < 100; i++) {
			cal.add(Calendar.HOUR_OF_DAY, 1);
			log.info("published event dage " + cal.getTime());
			Object[] payload = DataBuilder.buildPayloadArray(adapter, null, cal.getTime().getTime(), 32000);
			dataPublisher.publish(stream, System.currentTimeMillis(), new Object[] { "dalet-nnp-in" }, null, payload);
		}

		log.info("Event published to 1nd stream");

		Thread.sleep(3000);
		dataPublisher.stop();
	}

	public static void setTrustStoreParams() {
		File filePath = new File("src/main/resources");
		if (!filePath.exists()) {
			filePath = new File("resources");
		}
		String trustStore = filePath.getAbsolutePath();
		System.setProperty("javax.net.ssl.trustStore", trustStore + "/client-truststore.jks");
		System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

	}

}
