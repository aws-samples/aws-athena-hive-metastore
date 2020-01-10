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

import org.junit.Test;

import static com.amazonaws.athena.hms.HiveMetaStoreConf.DEFAULT_HMS_HANDLER_NAME_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestHiveMetaStoreConf {

  @Test
  public void testLoad() {
    // load configuration from hms.properties file
    HiveMetaStoreConf conf = HiveMetaStoreConf.load();
    assertNotNull(conf);
    assertFalse(conf.isUseSasl());
    assertFalse(conf.isUseFramedTransport());
    assertFalse(conf.isUseCompactProtocol());
    assertFalse(conf.isKerberosEnabled());
    assertNull(conf.getMetastoreKerberosPrincipal());
    assertEquals("thrift://my-hms-host:9083", conf.getMetastoreUri());
    assertEquals("s3://my-hms/lambda/functions/spill", conf.getResponseSpillLocation());
    assertEquals(4194304L, conf.getResponseSpillThreshold());
    assertEquals(DEFAULT_HMS_HANDLER_NAME_PREFIX, conf.getHandlerNamePrefix());
  }
}
