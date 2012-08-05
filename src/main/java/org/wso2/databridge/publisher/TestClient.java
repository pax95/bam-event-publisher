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
package org.wso2.databridge.publisher;

import java.io.File;
import java.net.MalformedURLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.AuthenticationException;
import org.wso2.carbon.databridge.commons.exception.DifferentStreamDefinitionAlreadyDefinedException;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.carbon.databridge.commons.exception.StreamDefinitionException;
import org.wso2.carbon.databridge.commons.exception.TransportException;
import org.wso2.carbon.databridge.commons.exception.UndefinedEventTypeException;

public class TestClient {
    Logger log = LoggerFactory.getLogger(TestClient.class);

    public static void main(String[] args)
            throws UndefinedEventTypeException, AgentException,
                   MalformedURLException, AuthenticationException,
                   MalformedStreamDefinitionException, StreamDefinitionException,
                   TransportException, InterruptedException,
                   DifferentStreamDefinitionAlreadyDefinedException {

        TestClient testClient = new TestClient();
        testClient.testSendingEvent();
    }


    public void testSendingEvent()
            throws MalformedURLException, AuthenticationException, TransportException,
                   AgentException, UndefinedEventTypeException,
                   DifferentStreamDefinitionAlreadyDefinedException,
                   InterruptedException,
                   MalformedStreamDefinitionException,
                   StreamDefinitionException {

        setTrustStoreParams();
        Thread.sleep(2000);

        //according to the convention the authentication port will be 7611+100= 7711 and its host will be the same
        DataPublisher dataPublisher = new DataPublisher("tcp://localhost:7611", "admin", "admin");
        String streamId1 = dataPublisher.defineStream("{" +
                                                      "  'name':'dk.dr.drip.AdapterStatistics'," +
                                                      "  'version':'1.0.1'," +
                                                      "  'nickName': 'Drip adapter Information'," +
                                                      "  'description': 'Some Desc'," +
                                                      "  'tags':['adapter', 'stats']," +
                                                      "  'metaData':[" +
                                                      "          {'name':'adapter','type':'STRING'}" +
                                                      "  ]," +
                                                      "  'payloadData':[" +
                                                      "          {'name':'adaptername','type':'STRING'}," +
                                                      "          {'name':'message','type':'STRING'}" +
                                                      "  ]" +
                                                      "}");
        log.info("1st stream defined: "+streamId1);

        //In this case correlation data is null
        dataPublisher.publish(streamId1, System.currentTimeMillis(), new Object[]{"dalet-bcr-in"}, null, new Object[]{"dalet-bcr-in", "<element><foo>asdf</foo></element>"});
        log.info("Event published to 1st stream");

        dataPublisher.publish(streamId1, System.currentTimeMillis(), new Object[]{"dalet-bcr-in"}, null, new Object[]{"dalet-bcr-in", "<element><foo>asdf2</foo></element>"});
        log.info("Event published to 2nd stream");

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
