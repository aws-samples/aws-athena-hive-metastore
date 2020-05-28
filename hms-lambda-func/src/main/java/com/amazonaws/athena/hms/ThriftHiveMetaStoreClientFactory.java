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

import org.apache.thrift.TException;

import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.net.URISyntaxException;

public class ThriftHiveMetaStoreClientFactory implements HiveMetaStoreClientFactory
{
  private final HiveMetaStoreConf conf;

  public ThriftHiveMetaStoreClientFactory()
  {
    // load configuration from a property file and override with environment variables
    this.conf = HiveMetaStoreConf.loadAndOverrideWithEnvironmentVariables();
  }

  @Override
  public HiveMetaStoreConf getConf()
  {
    return conf;
  }

  private ThriftHiveMetaStoreClient createClient()
  {
    try {
      // create the thrift Hive Metastore client
      return new ThriftHiveMetaStoreClient(conf.toHiveConf());
    }
    catch (TException | IOException | InterruptedException | LoginException | URISyntaxException e) {
      throw new RuntimeException("Failed to create HiveMetaStoreClient", e);
    }
  }

  @Override
  public HiveMetaStoreClient getHiveMetaStoreClient()
  {
    return createClient();
  }

  @Override
  public HandlerProvider getHandlerProvider()
  {
    // customer could override this method to provide their own HandlerProvider
    return new ApiNameHandlerProvider(new ApiHelper());
  }
}
