/*-
 * #%L
 * hms-lambda-func
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.auth.login.LoginException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// The thrift client to communicate with Hive Metastore via Thrift APIs
public class ThriftHiveMetaStoreClient implements HiveMetaStoreClient
{
  private static final String HIVE_SITE = "hive-site.xml";
  private static final String CORE_SITE = "core-site.xml";
  private static final String HADOOP_RPC_PROTECTION = "hadoop.rpc.protection";
  private static final long SOCKET_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(600);
  private static final AtomicInteger connectionCount = new AtomicInteger(0);

  private ThriftHiveMetastore.Iface client;
  private TTransport transport;
  private URI serverURI;

  public URI getServerURI()
  {
    return serverURI;
  }

  @Override
  public String toString()
  {
    return serverURI.toString();
  }

  public ThriftHiveMetaStoreClient(HiveConf conf)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException
  {
    String metastoreUri = conf.getVar(HiveConf.ConfVars.METASTOREURIS);
    getClient(new URI(metastoreUri), conf);
  }

  public ThriftHiveMetaStoreClient(URI uri, HiveConf conf)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException
  {
    getClient(uri, conf);
  }

  private void getClient(URI uri, HiveConf conf)
      throws TException, IOException, InterruptedException, URISyntaxException, LoginException
  {
    getClient(uri, conf, false);
  }

  private void getClient(URI uri, HiveConf conf, boolean kerberosEnabled)
      throws TException, IOException, InterruptedException, URISyntaxException, LoginException
  {
    // Pick up the first URI from the list of available URIs
    serverURI = uri != null ?
        uri :
        new URI(conf.getVar(HiveConf.ConfVars.METASTOREURIS).split(",")[0]);

    if (kerberosEnabled) {
      String principal = conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
      if (principal == null) {
        open(conf, serverURI);
        return;
      }

      // Kerberos magic
      Configuration hadoopConf = new Configuration();
      addResourceFromClassPath(hadoopConf, CORE_SITE);
      addResourceFromClassPath(hadoopConf, HIVE_SITE);

      System.out.println(HADOOP_RPC_PROTECTION + ": " + hadoopConf.get(HADOOP_RPC_PROTECTION));
      conf.set(HADOOP_RPC_PROTECTION, hadoopConf.get(HADOOP_RPC_PROTECTION));

      UserGroupInformation.setConfiguration(hadoopConf);
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      loginUser.checkTGTAndReloginFromKeytab();

      loginUser.doAs((PrivilegedExceptionAction<TTransport>)
          () -> open(conf, serverURI));

      String keytab = conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE);
    }
    else {
      open(conf, serverURI);
    }
  }

  private void addResource(Configuration conf, String filePath) throws MalformedURLException
  {
    File f = new File(filePath);
    if (f.exists() && !f.isDirectory()) {
      conf.addResource(f.toURI().toURL());
    }
  }

  private void addResourceFromClassPath(Configuration conf, String fileName) throws MalformedURLException
  {
    InputStream in = getClass().getResourceAsStream("/" + fileName);
    conf.addResource(in);
  }

  public boolean dbExists(String dbName) throws TException
  {
    return getDatabaseNames(dbName).contains(dbName);
  }

  public boolean tableExists(String dbName, String tableName) throws TException
  {
    return getTableNames(dbName, tableName).contains(tableName);
  }

  public Database getDatabase(String dbName) throws TException
  {
    return client.get_database(dbName);
  }

  public Set<String> getDatabaseNames(String filter) throws TException
  {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.get_all_databases());
    }
    client.shutdown();
    return client.get_all_databases()
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  // fetch all database objects in one call to avoid multiple lambda apis calls
  public List<Database> getDatabases(String filter) throws TException
  {
    List<Database> databases = new ArrayList<>();
    Set<String> databaseNames = getDatabaseNames(filter);
    if (databaseNames != null && !databaseNames.isEmpty()) {
      for (String databaseName : databaseNames) {
        databases.add(getDatabase(databaseName));
      }
    }

    return databases;
  }

  @Override
  public List<Database> getDatabasesByNames(List<String> dbNames) throws TException
  {
    List<Database> databases = new ArrayList<>();
    if (dbNames != null && !dbNames.isEmpty()) {
      for (String databaseName : dbNames) {
        databases.add(getDatabase(databaseName));
      }
    }

    return databases;
  }

  public Set<String> getTableNames(String dbName, String filter) throws TException
  {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.get_all_tables(dbName));
    }
    return client.get_all_tables(dbName)
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  public List<Table> getTablesByNames(String dbName, List<String> tableNames)
      throws TException
  {
    return client.get_table_objects_by_name(dbName, tableNames);
  }

  public boolean createDatabase(String name) throws TException
  {
    return createDatabase(name, null, null, null);
  }

  public boolean createDatabase(String name,
                                String description,
                                String location,
                                Map<String, String> params)
      throws TException
  {
    Database db = new Database(name, description, location, params);
    client.create_database(db);
    return true;
  }

  public boolean createDatabase(Database db) throws TException
  {
    client.create_database(db);
    return true;
  }

  public boolean dropDatabase(String dbName, boolean deleteData, boolean cascade) throws TException
  {
    client.drop_database(dbName, deleteData, cascade);
    return true;
  }

  public boolean createTable(Table table) throws TException
  {
    client.create_table(table);
    return true;
  }

  public boolean dropTable(String dbName, String tableName) throws TException
  {
    client.drop_table(dbName, tableName, false);
    return true;
  }

  public Table getTable(String dbName, String tableName) throws TException
  {
    return client.get_table(dbName, tableName);
  }

  public Partition createPartition(Table table, List<String> values) throws TException
  {
    return client.add_partition(new PartitionBuilder(table).withValues(values).build());
  }

  public Partition addPartition(Partition partition) throws TException
  {
    return client.add_partition(partition);
  }

  public void addPartitions(List<Partition> partitions) throws TException
  {
    client.add_partitions(partitions);
  }

  public Long getCurrentNotificationId() throws TException
  {
    return client.get_current_notificationEventId().getEventId();
  }

  public List<String> getPartitionNames(String dbName, String tableName, short maxSize) throws TException
  {
    return client.get_partition_names(dbName, tableName, maxSize);
  }

  public boolean dropPartition(String dbName, String tableName,
                               List<String> arguments)
      throws TException
  {
    return client.drop_partition(dbName, tableName, arguments, false);
  }

  public List<Partition> getPartitions(String dbName, String tableName, short maxSize) throws TException
  {
    return client.get_partitions(dbName, tableName, maxSize);
  }

  public List<Partition> getPartitionsByFilter(String dbName, String tableName, String partitionFilter, short maxSize) throws TException
  {
    return client.get_partitions_by_filter(dbName, tableName, partitionFilter, maxSize);
  }

  public DropPartitionsResult dropPartitions(String dbName, String tableName,
                                             List<String> partNames) throws TException
  {
    if (partNames == null) {
      return dropPartitions(dbName, tableName, getPartitionNames(dbName, tableName, (short) -1));
    }
    if (partNames.isEmpty()) {
      return null;
    }
    return client.drop_partitions_req(new DropPartitionsRequest(dbName,
        tableName, RequestPartsSpec.names(partNames)));
  }

  public List<Partition> getPartitionsByNames(String dbName, String tableName,
                                              List<String> names) throws TException
  {
    if (names == null) {
      return client.get_partitions_by_names(dbName, tableName,
          getPartitionNames(dbName, tableName, (short) -1));
    }
    return client.get_partitions_by_names(dbName, tableName, names);
  }

  public boolean alterTable(String dbName, String tableName, Table newTable)
      throws TException
  {
    client.alter_table(dbName, tableName, newTable);
    return true;
  }

  public boolean alterDatabase(String dbName, Database database)
          throws TException
  {
    client.alter_database(dbName, database);
    return true;
  }

  public void alterPartition(String dbName, String tableName,
                             Partition partition) throws TException
  {
    client.alter_partition(dbName, tableName, partition);
  }

  public void alterPartitions(String dbName, String tableName,
                              List<Partition> partitions) throws TException
  {
    client.alter_partitions(dbName, tableName, partitions);
  }

  public void appendPartition(String dbName, String tableName,
                              List<String> partitionValues) throws TException
  {
    client.append_partition_with_environment_context(dbName, tableName, partitionValues, null);
  }

  public void renamePartition(final String dbName, final String tableName, final List<String> partVals, final Partition newPart) throws TException
  {
    client.rename_partition(dbName, tableName, partVals, newPart);
  }

  public boolean listPartitionsByExpr(String dbName, String tableName,
                                      byte[] expr, String defaultPartitionName, short maxParts, List<Partition> partitions) throws TException
  {
    PartitionsByExprRequest req = buildPartitionsByExprRequest(dbName, tableName, expr, defaultPartitionName,
            maxParts);

    PartitionsByExprResult r = client.get_partitions_by_expr(req);
    if (partitions == null) {
      partitions = new ArrayList<>();
    }

    partitions.addAll(r.getPartitions());
    return !r.isSetHasUnknownPartitions() || r.isHasUnknownPartitions();
  }

  private TTransport open(HiveConf conf, URI uri) throws
      TException, IOException, LoginException
  {
    boolean useSasl = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);
    boolean useSSL = conf.getBoolean("hive.metastore.use.SSL", false);
    boolean useFramedTransport = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
    String tokenSig = conf.getVar(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE);
    String trustStorePath = conf.get("hive.metastore.ssl.truststore.path");
    String trustStorePassword = conf.get("hive.metastore.ssl.truststore.password");

    if (useSSL) {
      if (trustStorePath != null && !trustStorePath.isEmpty() &&
              trustStorePassword != null && !trustStorePassword.isEmpty()) {
        return createSSLConnection(conf, uri, useSasl, useFramedTransport, useCompactProtocol, tokenSig, trustStorePath, trustStorePassword);
      }
      else {
        throw new IllegalArgumentException(
                "SSL is enabled but truststore configuration is missing.\n" +
                        "Please set environment variables:\n" +
                        "  - HMS_SSL_TRUSTSTORE_PATH (path to truststore file)\n" +
                        "  - HMS_SSL_TRUSTSTORE_PASSWORD (truststore password)\n" +
                        "Valid path formats:\n" +
                        "  - /tmp/truststore.jks (local file in Lambda)\n" +
                        "  - s3://bucket/path/truststore.jks (will be downloaded from S3)"
        );
      }
    }
    else {
      // Create a plain socket connection
      TTransport transport = new TSocket(uri.getHost(), uri.getPort(), (int) SOCKET_TIMEOUT_MS);
      return setupClientConnection(conf, uri, transport, useSasl, useFramedTransport, useCompactProtocol, tokenSig, false);
    }
  }

  private TTransport setupClientConnection(HiveConf conf, URI uri, TTransport transport, boolean useSasl, boolean useFramedTransport,
                                           boolean useCompactProtocol, String tokenSig, boolean isSSL) throws TException, IOException, LoginException
  {
    try {
      if (useSasl) {
        HadoopThriftAuthBridge.Client authBridge =
                ShimLoader.getHadoopThriftAuthBridge().createClient();
        // tokenSig could be null
        String tokenStrForm = Utils.getTokenStrForm(tokenSig);
        if (tokenStrForm != null) {
          // authenticate using delegation tokens via the "DIGEST" mechanism
          transport = authBridge.createClientTransport(null, uri.getHost(),
                  "DIGEST", tokenStrForm, transport, MetaStoreUtils.getMetaStoreSaslProperties(conf));
        }
        else {
          String principalConfig = conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
          transport = authBridge.createClientTransport(
                  principalConfig, uri.getHost(), "KERBEROS", null,
                  transport, MetaStoreUtils.getMetaStoreSaslProperties(conf));
        }
      }

      transport = useFramedTransport ? new TFastFramedTransport(transport) : transport;
      TProtocol protocol = useCompactProtocol ?
              new TCompactProtocol(transport) :
              new TBinaryProtocol(transport);
      client = new ThriftHiveMetastore.Client(protocol);
      if (!transport.isOpen()) {
        transport.open();
      }
      if (!useSasl && conf.getBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI)) {
        UserGroupInformation ugi = Utils.getUGI();
        client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
      }

      return transport;
    }
    catch (TTransportException te) {
      throw new RuntimeException("Failed to setup client due to SSL configuration mismatch with server.", te);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to setup client", e);
    }
  }

  private TTransport createSSLConnection(HiveConf conf, URI uri, boolean useSasl, boolean useFramedTransport, boolean useCompactProtocol,
                                         String tokenSig, String trustStorePath, String trustStorePassword) throws TException, IOException, LoginException
  {
    try {
      String resolvedPath = resolveTrustStorePath(trustStorePath, trustStorePassword);
      File truststoreFile = new File(resolvedPath);
      if (!truststoreFile.exists()) {
        throw new IOException("Truststore file not found at: " + resolvedPath);
      }
      if (!truststoreFile.canRead()) {
        throw new IOException("Cannot read truststore file at: " + resolvedPath);
      }

      System.out.println("Using truststore at: " + resolvedPath);

      // Create SSL connection with the truststore
      TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters();
      params.setTrustStore(resolvedPath, trustStorePassword);
      params.requireClientAuth(false);

      transport = TSSLTransportFactory.getClientSocket(uri.getHost(), uri.getPort(), (int) SOCKET_TIMEOUT_MS, params);

      System.out.println("SSL connection established successfully");
    }
    catch (Exception e) {
      throw new TException("Failed to create SSL connection: " + e.getMessage(), e);
    }

    return setupClientConnection(conf, uri, transport, useSasl, useFramedTransport, useCompactProtocol, tokenSig, true);
  }

  private String resolveTrustStorePath(String trustStorePath, String trustStorePassword)
          throws IOException
  {
    if (trustStorePath == null || trustStorePath.isEmpty()) {
      throw new IllegalArgumentException("Truststore path cannot be empty");
    }

    // Handle S3 paths - download to /tmp
    if (trustStorePath.startsWith("s3://")) {
      return downloadTruststoreFromS3(trustStorePath);
    }

    // Handle /tmp paths (Lambda's writable directory)
    if (trustStorePath.startsWith("/tmp/")) {
      return trustStorePath;
    }

    // Handle /opt paths (Lambda layers - read-only)
    if (trustStorePath.startsWith("/opt/")) {
      return trustStorePath;
    }

    File file = new File(trustStorePath);
    if (file.isAbsolute() && file.exists()) {
      return trustStorePath;
    }

    throw new IllegalArgumentException(
            "Invalid truststore path: " + trustStorePath + "\n" +
                    "Lambda can only access:\n" +
                    "  - /tmp/truststore.jks (local file in Lambda /tmp directory)\n" +
                    "  - /opt/truststore.jks (file from Lambda layer)\n" +
                    "  - s3://bucket/path/truststore.jks (will be downloaded to /tmp)\n" +
                    "Current path does not match any valid format."
    );
  }

  private String downloadTruststoreFromS3(String s3Path) throws IOException
  {
    System.out.println("Downloading truststore from S3: " + s3Path);
    if (!s3Path.startsWith("s3://")) {
      throw new IllegalArgumentException("Invalid S3 path format: " + s3Path);
    }

    String pathWithoutProtocol = s3Path.substring(5);
    int firstSlash = pathWithoutProtocol.indexOf('/');

    if (firstSlash == -1) {
      throw new IllegalArgumentException(
              "Invalid S3 path format: " + s3Path + "\n" +
                      "Expected format: s3://bucket-name/path/to/truststore.jks"
      );
    }

    String bucket = pathWithoutProtocol.substring(0, firstSlash);
    String key = pathWithoutProtocol.substring(firstSlash + 1);

    if (bucket.isEmpty() || key.isEmpty()) {
      throw new IllegalArgumentException("S3 bucket or key cannot be empty in path: " + s3Path);
    }

    String fileName = key.substring(key.lastIndexOf('/') + 1);
    String localPath = "/tmp/" + fileName;

    try {
      AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
      File localFile = new File(localPath);

      System.out.println("Downloading from bucket: " + bucket + ", key: " + key);
      s3Client.getObject(new GetObjectRequest(bucket, key), localFile);

      System.out.println("Successfully downloaded truststore to: " + localPath);
      return localPath;
    }
    catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 403) {
        throw new IOException(
                "Access denied downloading truststore from S3: " + s3Path + "\n" +
                        "Ensure Lambda execution role has s3:GetObject permission for this object.\n" +
                        "Required IAM policy:\n" +
                        "{\n" +
                        "  \"Effect\": \"Allow\",\n" +
                        "  \"Action\": \"s3:GetObject\",\n" +
                        "  \"Resource\": \"arn:aws:s3:::" + bucket + "/" + key + "\"\n" +
                        "}", e
        );
      }
      else if (e.getStatusCode() == 404) {
        throw new IOException(
                "Truststore not found in S3: " + s3Path + "\n" +
                        "Please verify the bucket and key exist.", e
        );
      }
      throw new IOException("Failed to download truststore from S3: " + e.getMessage(), e);
    }
    catch (Exception e) {
      throw new IOException(
              "Unexpected error downloading truststore from S3: " + s3Path + "\n" +
                      "Error: " + e.getMessage(), e
      );
    }
  }

  static class PartitionBuilder
  {
    private final Table table;
    private List<String> values;
    private String location;
    private Map<String, String> parameters = new HashMap<>();

    private PartitionBuilder()
    {
      table = null;
    }

    PartitionBuilder(Table table)
    {
      this.table = table;
    }

    PartitionBuilder withValues(List<String> values)
    {
      this.values = new ArrayList<>(values);
      return this;
    }

    PartitionBuilder withLocation(String location)
    {
      this.location = location;
      return this;
    }

    PartitionBuilder withParameter(String name, String value)
    {
      parameters.put(name, value);
      return this;
    }

    PartitionBuilder withParameters(Map<String, String> params)
    {
      parameters = params;
      return this;
    }

    Partition build()
    {
      Partition partition = new Partition();
      List<String> partitionNames = table.getPartitionKeys()
          .stream()
          .map(FieldSchema::getName)
          .collect(Collectors.toList());
      if (partitionNames.size() != values.size()) {
        throw new RuntimeException("Partition values do not match table schema");
      }
      List<String> spec = IntStream.range(0, values.size())
          .mapToObj(i -> partitionNames.get(i) + "=" + values.get(i))
          .collect(Collectors.toList());

      partition.setDbName(table.getDbName());
      partition.setTableName(table.getTableName());
      partition.setParameters(parameters);
      partition.setValues(values);
      partition.setSd(table.getSd().deepCopy());
      if (this.location == null) {
        partition.getSd().setLocation(table.getSd().getLocation() + "/" + Joiner.on("/").join(spec));
      }
      else {
        partition.getSd().setLocation(location);
      }
      return partition;
    }
  }

  private PartitionsByExprRequest buildPartitionsByExprRequest(String dbName, String tableName, byte[] expr,
                                                               String defaultPartitionName, short maxParts)
  {
    PartitionsByExprRequest req = new PartitionsByExprRequest(
            dbName, tableName, ByteBuffer.wrap(expr));

    if (defaultPartitionName != null) {
      req.setDefaultPartitionName(defaultPartitionName);
    }
    if (maxParts >= 0) {
      req.setMaxParts(maxParts);
    }
    return req;
  }

  @Override
  public void close(Context context)
  {
    try {
      if (client != null) {
        client.shutdown();
        if ((transport == null) || !transport.isOpen()) {
          final int newCount = connectionCount.decrementAndGet();
          context.getLogger().log("Closed a connection to metastore, current connections: " +
                  newCount);
        }
      }
    }
    catch (TException e) {
      context.getLogger().log("Unable to shutdown metastore client. Will try closing transport directly." + e);
    }
    // Transport would have got closed via client.shutdown(), so we don't need this, but
    // just in case, we make this call.
    if ((transport != null) && transport.isOpen()) {
      transport.close();
      final int newCount = connectionCount.decrementAndGet();
      context.getLogger().log("Closed a connection to metastore, current connections: " +
              newCount);
    }
  }

  public void refreshClient(HiveConf conf, Context context) throws TException, LoginException, IOException, URISyntaxException, InterruptedException
  {
    close(context);
    ThriftHiveMetastore.Iface clientBeforeRefresh = client;
    getClient(new URI(conf.getVar(HiveConf.ConfVars.METASTOREURIS)), conf);
    if (client == clientBeforeRefresh) {
      context.getLogger().log("Client refresh didn't work");
    }
    else {
      context.getLogger().log("Client refresh worked");
    }
  }
}
