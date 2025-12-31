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
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.security.KeyStore;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class ThriftHiveMetaStoreClientTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String originalTruststorePath;
  private String originalTruststorePassword;
  private URI testUri;

  @Before
  public void setUp() throws Exception {
    testUri = new URI("thrift://test-host:9083");
    originalTruststorePath = System.getenv("HMS_SSL_TRUSTSTORE_PATH");
    originalTruststorePassword = System.getenv("HMS_SSL_TRUSTSTORE_PASSWORD");
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
        fail("Test: testOpenPlainSocketSuccessWithoutSASL failed - plain socket connection should succeed, but got: "
                + e.getMessage());
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
        assertFalse("Should not reach SSL fallback, so exceptions should not be related to SSL",
          e.getMessage() != null && e.getMessage().contains("SSL"));
        fail("Test: testOpenPlainSocketSuccessWithSASL failed - plain socket connection should succeed, but got: "
                + e.getMessage());
      }
    }
  }

  @Test
  public void testOpenSSLEnabledButNoEnvVars() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, false);
    conf.setBoolean("hive.metastore.use.SSL", true);
    URI uri = new URI("thrift://localhost:9083");

    try {
      new ThriftHiveMetaStoreClient(uri, conf);
      fail("Test: testOpenSSLEnabledButNoEnvVars failed - test should throw IllegalArgumentException but did not.");
    } catch (IllegalArgumentException e) {
      assertTrue("Should mention missing truststore configuration",
              e.getMessage().contains("SSL is enabled but truststore configuration is missing"));
      assertTrue("Should mention HMS_SSL_TRUSTSTORE_PATH",
              e.getMessage().contains("HMS_SSL_TRUSTSTORE_PATH"));
    }
  }

  @Test
  public void testOpenSSLEnabledWithEmptyEnvVars() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, false);
    conf.setBoolean("hive.metastore.use.SSL", true);
    URI uri = new URI("thrift://localhost:9083");
    conf.set("hive.metastore.ssl.truststore.path", "");
    conf.set("hive.metastore.ssl.truststore.password", "");
    try {
      new ThriftHiveMetaStoreClient(uri, conf);
      fail("Test: testOpenSSLEnabledWithEmptyEnvVars failed - Test should throw IllegalArgumentException but did not.");
    } catch (IllegalArgumentException e) {
      assertTrue("Should mention missing truststore configuration",
              e.getMessage().contains("SSL is enabled but truststore configuration is missing"));
    }
  }

  @Test
  public void testOpenSSLEnabledWithEnvVarsWithNoTruststoreFile() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, false);
    conf.setBoolean("hive.metastore.use.SSL", true);
    URI uri = new URI("thrift://localhost:9083");
    conf.set("hive.metastore.ssl.truststore.path", "/tmp/truststore.jks");
    conf.set("hive.metastore.ssl.truststore.password", "password");
    try {
      new ThriftHiveMetaStoreClient(uri, conf);
      fail("Test: testOpenSSLEnabledWithEnvVarsWithNoTruststoreFile failed - Test should throw TException but did not.");
    } catch (TException e) {
      assertTrue("Should mention failure to create SSL connection due to truststore file not found",
              e.getMessage().contains("Truststore file not found at: /tmp/truststore.jks"));
    }
  }

  @Test
  public void testOpenSSLConnectionWithValidTruststore() throws Exception {
    // Create a real truststore file
    File truststore = tempFolder.newFile("truststore.jks");
    createDummyTruststore(truststore, "password");

    HiveConf conf = new HiveConf();
    conf.setBoolean("hive.metastore.use.SSL", true);
    conf.set("hive.metastore.ssl.truststore.path", truststore.getAbsolutePath());
    conf.set("hive.metastore.ssl.truststore.password", "password");
    URI uri = new URI("thrift://localhost:9083");

    try {
      new ThriftHiveMetaStoreClient(uri, conf);
      fail("Test: testOpenSSLEnabledWithEnvVarsWithNoTruststoreFile failed - Test should throw fail with connection error (no server running)");
    } catch (Exception e) {
      // Should get past truststore validation and SSL setup
      // Should fail at actual connection to server
      assertFalse("Should not fail on truststore validation",
              e.getMessage() != null && e.getMessage().contains("Truststore file not found"));
      assertFalse("Should not fail on truststore read",
              e.getMessage() != null && e.getMessage().contains("Cannot read truststore"));
      // Should fail with connection error or SSL handshake error
      assertTrue("Should fail with connection-related error",
              e.getMessage() != null &&
                      (e.getMessage().equals("Failed to create SSL connection: Could not connect to localhost on port 9083")));
    }
  }

  @Test
  public void testOpenSSLConnectionSuccessful() throws Exception {
    File truststore = tempFolder.newFile("truststore.jks");
    createDummyTruststore(truststore, "password");

    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, false);
    conf.setBoolean("hive.metastore.use.SSL", true);
    conf.set("hive.metastore.ssl.truststore.path", truststore.getAbsolutePath());
    conf.set("hive.metastore.ssl.truststore.password", "password");
    URI uri = new URI("thrift://localhost:9083");

    try (MockedStatic<TSSLTransportFactory> sslMock = mockStatic(TSSLTransportFactory.class)) {
      TSocket mockSSLSocket = mock(TSocket.class);
      when(mockSSLSocket.isOpen()).thenReturn(false, true);
      doNothing().when(mockSSLSocket).open();

      sslMock.when(() -> TSSLTransportFactory.getClientSocket(
              anyString(), anyInt(), anyInt(), any()
      )).thenReturn(mockSSLSocket);

      try {
        new ThriftHiveMetaStoreClient(uri, conf);
        fail("Should fail when trying to set_ugi (no real Hive client)");
      } catch (Exception e) {
        // Should not fail on SSL connection but should fail on further setting up of the client
        assertFalse("Should not fail on SSL connection exception",
                e.getMessage() != null && e.getMessage().contains("Failed to create SSL connection"));
        assertFalse("Should not fail on truststore exception",
                e.getMessage() != null && e.getMessage().contains("Truststore"));
        // Since an SSL connection cannot be mocked, further setup of socket will fail in the test
        // but will verify SSL connection was established successfully
        assertTrue("Should fail with exception on client setup with ugi",
                e.getMessage() != null &&
                        (e.getMessage().contains("Failed to setup client")));
      }
    }
  }

  private void createDummyTruststore(File file, String password) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, password.toCharArray());
    try (FileOutputStream fos = new FileOutputStream(file)) {
      keyStore.store(fos, password.toCharArray());
    }
  }
}
