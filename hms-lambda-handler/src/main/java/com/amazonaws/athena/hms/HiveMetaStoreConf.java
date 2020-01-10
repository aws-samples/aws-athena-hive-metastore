/*-
 * #%L
 * hms-lambda-handler
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

import com.amazonaws.athena.conf.Configuration;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;

public class HiveMetaStoreConf
{
  public static final String HMS_PROPERTIES = "hms.properties";
  public static final String HMS_KERBEROS_ENABLED = "hive.metastore.kerberos.enabled";
  public static final String HMS_RESPONSE_SPILL_LOCATION = "hive.metastore.response.spill.location";
  public static final String HMS_RESPONSE_SPILL_THRESHOLD = "hive.metastore.response.spill.threshold";
  public static final String HMS_HANDLER_NAME_PREFIX = "hive.metastore.handler.name.prefix";
  public static final String DEFAULT_HMS_HANDLER_NAME_PREFIX = "com.amazonaws.athena.hms.handler.";
  public static final long DEFAULT_HMS_RESPONSE_SPILL_THRESHOLD = 4 * 1024 * 1024; // 4MB
  public static final String ENV_HMS_URIS = "HMS_URIS";
  public static final String ENV_SPILL_LOCATION = "SPILL_LOCATION";

  // Hive configuration: "hive.metastore.sasl.enabled"
  private boolean useSasl = false;

  // Hive configuration: "hive.metastore.thrift.framed.transport.enabled"
  private boolean useFramedTransport;

  // Hive configuration: "hive.metastore.thrift.compact.protocol.enabled"
  private boolean useCompactProtocol;

  // Hive configuration: "hive.metastore.token.signature"
  private String tokenSig;

  private boolean kerberosEnabled;

  // Hive configuration: "hive.metastore.kerberos.principal"
  private String metastoreKerberosPrincipal;

  // Hive configuration: "hive.metastore.execute.setugi"
  private boolean metastoreSetUgi;

  // Hive configuration: "hive.metastore.uris"
  private String metastoreUri;

  // Hive configuration: "hive.metastore.kerberos.keytab.file"
  private String keytabFile;

  // the threshold to decide whether to spill the response to s3 or not
  private long responseSpillThreshold;

  // the root s3 path to store the spilled response file
  private String responseSpillLocation;

  // the handler name prefix
  private String handlerNamePrefix;

  public boolean isKerberosEnabled()
  {
    return kerberosEnabled;
  }

  public void setKerberosEnabled(boolean kerberosEnabled)
  {
    this.kerberosEnabled = kerberosEnabled;
  }

  public String getMetastoreUri()
  {
    return metastoreUri;
  }

  public void setMetastoreUri(String metastoreUri)
  {
    this.metastoreUri = metastoreUri;
  }

  public boolean isUseSasl()
  {
    return useSasl;
  }

  public void setUseSasl(boolean useSasl)
  {
    this.useSasl = useSasl;
  }

  public boolean isUseFramedTransport()
  {
    return useFramedTransport;
  }

  public void setUseFramedTransport(boolean useFramedTransport)
  {
    this.useFramedTransport = useFramedTransport;
  }

  public boolean isUseCompactProtocol()
  {
    return useCompactProtocol;
  }

  public void setUseCompactProtocol(boolean useCompactProtocol)
  {
    this.useCompactProtocol = useCompactProtocol;
  }

  public String getTokenSig()
  {
    return tokenSig;
  }

  public void setTokenSig(String tokenSig)
  {
    this.tokenSig = tokenSig;
  }

  public String getMetastoreKerberosPrincipal()
  {
    return metastoreKerberosPrincipal;
  }

  public void setMetastoreKerberosPrincipal(String metastoreKerberosPrincipal)
  {
    this.metastoreKerberosPrincipal = metastoreKerberosPrincipal;
  }

  public boolean isMetastoreSetUgi()
  {
    return metastoreSetUgi;
  }

  public void setMetastoreSetUgi(boolean metastoreSetUgi)
  {
    this.metastoreSetUgi = metastoreSetUgi;
  }

  public String getKeytabFile()
  {
    return keytabFile;
  }

  public void setKeytabFile(String keytabFile)
  {
    this.keytabFile = keytabFile;
  }

  public String getResponseSpillLocation()
  {
    return responseSpillLocation;
  }

  public void setResponseSpillLocation(String responseSpillLocation)
  {
    this.responseSpillLocation = responseSpillLocation;
  }

  public long getResponseSpillThreshold()
  {
    return responseSpillThreshold;
  }

  public void setResponseSpillThreshold(long responseSpillThreshold)
  {
    this.responseSpillThreshold = responseSpillThreshold;
  }

  public String getHandlerNamePrefix()
  {
    return handlerNamePrefix;
  }

  public void setHandlerNamePrefix(String handlerNamePrefix)
  {
    this.handlerNamePrefix = handlerNamePrefix;
  }

  /*
   * convert this configuration class to an HiveConf object
   *
   * @return HiveConf
   */
  public HiveConf toHiveConf()
  {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, useSasl);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT, useFramedTransport);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL, useCompactProtocol);
    if (tokenSig != null) {
      conf.setVar(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE, tokenSig);
    }
    if (metastoreKerberosPrincipal != null) {
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, metastoreKerberosPrincipal);
    }
    if (keytabFile != null) {
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keytabFile);
    }
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, metastoreSetUgi);

    return conf;
  }

  /*
   * load the HiveMetaStoreConf from a property file "hms.properties"
   *
   * @return HiveMetaStoreConf
   */
  public static HiveMetaStoreConf load()
  {
    Configuration hmsConf = Configuration.loadDefaultFromClasspath(HMS_PROPERTIES);

    HiveMetaStoreConf conf = new HiveMetaStoreConf();
    conf.setMetastoreUri(hmsConf.getProperty(HiveConf.ConfVars.METASTOREURIS.varname));
    conf.setUseSasl(hmsConf.getBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, false));
    conf.setUseFramedTransport(hmsConf.getBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT.varname, false));
    conf.setUseCompactProtocol(hmsConf.getBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL.varname, false));
    conf.setTokenSig(hmsConf.getProperty(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE.varname));
    conf.setKerberosEnabled(hmsConf.getBoolean(HMS_KERBEROS_ENABLED, false));
    conf.setMetastoreKerberosPrincipal(hmsConf.getProperty(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname));
    conf.setKeytabFile(hmsConf.getProperty(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE.varname));
    conf.setMetastoreSetUgi(hmsConf.getBoolean(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname, true));
    conf.setResponseSpillLocation(hmsConf.getProperty(HMS_RESPONSE_SPILL_LOCATION));
    conf.setResponseSpillThreshold(hmsConf.getLong(HMS_RESPONSE_SPILL_THRESHOLD, DEFAULT_HMS_RESPONSE_SPILL_THRESHOLD));
    conf.setHandlerNamePrefix(hmsConf.getString(HMS_HANDLER_NAME_PREFIX, DEFAULT_HMS_HANDLER_NAME_PREFIX));
    return conf;
  }

  /*
   * first load the property file and then override some properties with environment variables
   * since Lambda function could pass in environment variables. In this way, we don't need to
   * recompile the source code for property changes
   *
   * @return HiveMetaStoreConf
   */
  public static HiveMetaStoreConf loadAndOverrideWithEnvironmentVariables()
  {
    HiveMetaStoreConf conf = load();
    // only support HMS_URIS and SPILL_LOCATION for now since most likely we only need to override them
    String hmsUris = System.getenv(ENV_HMS_URIS);
    if (!Strings.isNullOrEmpty(hmsUris)) {
      conf.setMetastoreUri(hmsUris);
    }
    String spillLocation = System.getenv(ENV_SPILL_LOCATION);
    if (!Strings.isNullOrEmpty(spillLocation)) {
      conf.setResponseSpillLocation(spillLocation);
    }

    return conf;
  }

  @Override
  public String toString()
  {
    return "{" +
        "useSasl: " + useSasl +
        ", useFramedTransport: " + useFramedTransport +
        ", useCompactProtocol: " + useCompactProtocol +
        ", tokenSig: '" + tokenSig + '\'' +
        ", kerberosEnabled: " + kerberosEnabled +
        ", metastoreKerberosPrincipal: '" + metastoreKerberosPrincipal + '\'' +
        ", metastoreSetUgi: " + metastoreSetUgi +
        ", metastoreUri: '" + metastoreUri + '\'' +
        ", keytabFile: '" + keytabFile + '\'' +
        ", responseSpillThreshold: " + responseSpillThreshold +
        ", responseSpillLocation: '" + responseSpillLocation + '\'' +
        ", handlerNamePrefix: '" + handlerNamePrefix + '\'' +
        '}';
  }
}
