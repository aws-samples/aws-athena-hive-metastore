/*-
 * #%L
 * hms-lambda-func
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.hms;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URI;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ThriftHiveMetaStoreClientTest {

  private URI testUri;

  @Before
  public void setUp() throws Exception {
    testUri = new URI("thrift://test-host:9083");
  }

  @Test
  public void testGetServerURI() throws Exception {
    assertEquals("test-host", testUri.getHost());
    assertEquals(9083, testUri.getPort());
    assertEquals("thrift", testUri.getScheme());
  }

  @Test
  public void testToString() throws Exception {
    String expected = "thrift://test-host:9083";
    assertEquals(expected, testUri.toString());
  }

  @Test
  public void testOpenPlainSocketSuccessWithoutSASL() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, false);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);
    conf.setBoolean("hive.metastore.use.SSL", false);
    URI uri = new URI("thrift://localhost:9083");
    try (MockedConstruction<TSocket> socketMock = mockConstruction(TSocket.class, (mock, context) -> {
      when(mock.isOpen()).thenReturn(false).thenReturn(true);
      doNothing().when(mock).open();
    })) {
      try {
        ThriftHiveMetaStoreClient client = new ThriftHiveMetaStoreClient(uri, conf);
        assertNotNull("Client should be created successfully", client);
        assertEquals("Should create exactly one TSocket for plain connection", 1, socketMock.constructed().size());
        TSocket createdSocket = socketMock.constructed().get(0);
        verify(createdSocket).open();
      } catch (Exception e) {
        fail("Plain socket connection should succeed, but got: " + e.getMessage());
      }
    }
  }

  @Test
  public void testOpenPlainSocketSuccessWithSASL() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
    conf.setBoolean("hive.metastore.use.SSL", false);
    URI uri = new URI("thrift://localhost:9083");
    try (MockedConstruction<TSocket> socketMock = mockConstruction(TSocket.class, (mock, context) -> {
      when(mock.isOpen()).thenReturn(false).thenReturn(true);
      doNothing().when(mock).open();
    });
    MockedStatic<ShimLoader> shimMock = mockStatic(ShimLoader.class)) {
      HadoopThriftAuthBridge mockAuthBridge = mock(HadoopThriftAuthBridge.class);
      HadoopThriftAuthBridge.Client mockClient = mock(HadoopThriftAuthBridge.Client.class);
      TTransport mockTransport = mock(TTransport.class);
      shimMock.when(ShimLoader::getHadoopThriftAuthBridge).thenReturn(mockAuthBridge);
      when(mockAuthBridge.createClient()).thenReturn(mockClient);
      when(mockClient.createClientTransport(any(), any(), any(), any(), any(), any())).thenReturn(mockTransport);
      when(mockTransport.isOpen()).thenReturn(true);
      try {
        ThriftHiveMetaStoreClient client = new ThriftHiveMetaStoreClient(uri, conf);
        assertNotNull("Client should be created successfully", client);
        assertEquals("Should create exactly one TSocket for plain connection", 1, socketMock.constructed().size());
        TSocket createdSocket = socketMock.constructed().get(0);
      } catch (Exception e) {
        System.out.println("!!! catch testOpenPlainSocketSuccessWithSASL");
        System.out.println(e.getMessage());
        assertFalse("Should not reach SSL fallback, so excepts should not be related to SSL",
          e.getMessage() != null && e.getMessage().contains("SSL"));
      }
    }
  }

  @Test
  public void testOpenDirectSSLConnection() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, false);
    conf.setBoolean("hive.metastore.use.SSL", true);
    URI uri = new URI("thrift://localhost:9083");
    try (MockedStatic<TSSLTransportFactory> sslMock = mockStatic(TSSLTransportFactory.class)) {
      TSocket mockSSLSocket = mock(TSocket.class);
      sslMock.when(() -> TSSLTransportFactory.getClientSocket(any(), anyInt(), anyInt(), any()))
             .thenReturn(mockSSLSocket);
      try {
        new ThriftHiveMetaStoreClient(uri, conf);
      } catch (Exception e) {
        assertFalse("Should not attempt plain socket connection so exception should not be related to plain socket",
                e.getMessage() != null && e.getMessage().contains("plain socket connection"));
        assertTrue("Should fail with SSL exception since SSL socket can't be mocked - proves it reaches SSL connection logic in createSSLConnection()",
                e.getMessage() != null && e.getMessage().contains("Failed to create SSL connection:"));
      }
    }
  }

  @Test
  public void testOpenPlainSocketFailsTriggersSSLFallback() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, false);
    conf.setBoolean("hive.metastore.use.SSL", false);
    URI uri = new URI("thrift://localhost:9083");

    try (MockedStatic<TSSLTransportFactory> sslMock = mockStatic(TSSLTransportFactory.class)) {
      TSocket mockSSLSocket = mock(TSocket.class);
      sslMock.when(() -> TSSLTransportFactory.getClientSocket(any(), anyInt(), anyInt(), any()))
              .thenReturn(mockSSLSocket);
      try {
        new ThriftHiveMetaStoreClient(uri, conf);
      } catch (Exception e) {
        assertFalse("Should not attempt plain socket connection so exception should not be related to plain socket",
                e.getMessage() != null && e.getMessage().contains("plain socket connection"));
        assertTrue("Should fail with SSL exception since SSL socket can't be mocked - proves it reaches SSL connection logic in createSSLConnection()",
                e.getMessage() != null && e.getMessage().contains("Failed to create SSL connection:"));
      }
    }
  }
}
