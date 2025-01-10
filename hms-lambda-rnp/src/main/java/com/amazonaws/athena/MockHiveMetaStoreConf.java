/*-
 * #%L
 * hms-lambda-rnp
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena;

import com.amazonaws.athena.conf.Configuration;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;

public class MockHiveMetaStoreConf extends HiveMetaStoreConf
{
  public static final String HMS_RESPONSE_RECORD_LOCATION = "hive.metastore.response.record.location";
  public static final String ENV_RECORD_LOCATION = "RECORD_LOCATION";

  // the root s3 path to store the recorded response file
  private String responseRecordLocation;

  public String getResponseRecordLocation()
  {
    return responseRecordLocation;
  }

  public void setResponseRecordLocation(String responseRecordLocation)
  {
    this.responseRecordLocation = responseRecordLocation;
  }

  public static MockHiveMetaStoreConf loadAndOverrideExtraEnvironmentVariables()
  {
    Configuration hmsConf = Configuration.loadDefaultFromClasspath(HMS_PROPERTIES);

    MockHiveMetaStoreConf conf = new MockHiveMetaStoreConf();
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
    conf.setResponseSpillLocation(hmsConf.getProperty(HMS_RESPONSE_RECORD_LOCATION));

    // override parameters with Lambda Environment variables
    String hmsUris = System.getenv(ENV_HMS_URIS);
    if (!Strings.isNullOrEmpty(hmsUris)) {
      conf.setMetastoreUri(hmsUris);
    }
    String spillLocation = System.getenv(ENV_SPILL_LOCATION);
    if (!Strings.isNullOrEmpty(spillLocation)) {
      conf.setResponseSpillLocation(spillLocation);
    }
    String recordLocation = System.getenv(ENV_RECORD_LOCATION);
    if (!Strings.isNullOrEmpty(recordLocation)) {
      conf.setResponseRecordLocation(recordLocation);
    }
    return conf;
  }

  @Override
  public String toString()
  {
    return "{" +
        "useSasl: " + isUseSasl() +
        ", useFramedTransport: " + isUseFramedTransport() +
        ", useCompactProtocol: " + isUseCompactProtocol() +
        ", tokenSig: '" + getTokenSig() + '\'' +
        ", kerberosEnabled: " + isKerberosEnabled() +
        ", metastoreKerberosPrincipal: '" + getMetastoreKerberosPrincipal() + '\'' +
        ", metastoreSetUgi: " + isMetastoreSetUgi() +
        ", metastoreUri: '" + getMetastoreUri() + '\'' +
        ", keytabFile: '" + getKeytabFile() + '\'' +
        ", responseSpillThreshold: " + getResponseSpillThreshold() +
        ", responseSpillLocation: '" + getResponseSpillLocation() + '\'' +
        ", responseRecordLocation: '" + getResponseRecordLocation() + '\'' +
        ", handlerNamePrefix: '" + getHandlerNamePrefix() + '\'' +
        '}';
  }
}
