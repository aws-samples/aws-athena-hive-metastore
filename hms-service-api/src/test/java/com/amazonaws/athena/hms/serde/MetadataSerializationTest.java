/*-
 * #%L
 * hms-service-api
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
package com.amazonaws.athena.hms.serde;

import com.amazonaws.athena.hms.AlterTableRequest;
import com.amazonaws.athena.hms.AlterTableResponse;
import com.amazonaws.athena.hms.ApiHelper;
import com.amazonaws.athena.hms.DbExistsRequest;
import com.amazonaws.athena.hms.DbExistsResponse;
import com.amazonaws.athena.hms.RequestContext;
import com.amazonaws.athena.hms.MetadataRequest;
import com.amazonaws.athena.hms.MetadataResponse;
import com.amazonaws.athena.hms.io.S3Helper;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataSerializationTest
{

  @Test
  public void testMetadataRequest() throws IOException
  {
    ApiHelper helper = new ApiHelper();
    ObjectMapper mapper = ObjectMapperFactory.create(helper, mock(S3Helper.class));
    AlterTableRequest alterTableRequest = new AlterTableRequest();
    alterTableRequest.setTableDesc("");
    alterTableRequest.setTableName("mytable");
    alterTableRequest.setDbName("mydb");
    RequestContext context = new RequestContext("test", "arn:aws:iam::12345678:user/user1", "12345678");
    MetadataRequest request =
        new MetadataRequest(context, helper.getApiName(AlterTableRequest.class, AlterTableResponse.class), alterTableRequest);
    String payload = mapper.writeValueAsString(request);
    MetadataRequest result = mapper.readValue(payload, MetadataRequest.class);
    assertNotNull(result);
    assertEquals("alterTable", request.getApiName());
    assertNotNull(request.getApiRequest());
    assertNotNull(request.getContext());
    assertEquals("test", request.getContext().getId());
    assertEquals("arn:aws:iam::12345678:user/user1", request.getContext().getPrincipal());
    assertEquals("12345678", request.getContext().getAccount());
    assertTrue(request.getApiRequest() instanceof AlterTableRequest);
    AlterTableRequest resultRequest = (AlterTableRequest) request.getApiRequest();
    assertEquals("", resultRequest.getTableDesc());
    assertEquals("mytable", resultRequest.getTableName());
    assertEquals("mydb", resultRequest.getDbName());
  }

  @Test
  public void testMetadataResponseDirect() throws IOException
  {
    ApiHelper helper = new ApiHelper();
    ObjectMapper mapper = ObjectMapperFactory.create(helper, mock(S3Helper.class));
    DbExistsResponse apiResponse = new DbExistsResponse();
    apiResponse.setExists(true);
    MetadataResponse response = new MetadataResponse(
        helper.getApiName(DbExistsRequest.class, DbExistsResponse.class), false, null, apiResponse);
    String payload = mapper.writeValueAsString(response);
    MetadataResponse result = mapper.readValue(payload, MetadataResponse.class);
    assertNotNull(result);
    assertEquals("dbExists", result.getApiName());
    assertFalse(result.isSpilled());
    assertNull(result.getSpillPath());
    assertNotNull(result.getApiResponse());
    assertTrue(result.getApiResponse() instanceof DbExistsResponse);
    DbExistsResponse resultResponse = (DbExistsResponse) result.getApiResponse();
    assertTrue(resultResponse.isExists());
  }

  @Test
  public void testMetadataResponseFromS3() throws IOException
  {
    AmazonS3 s3Client = mock(AmazonS3.class);
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream("{\"exists\":\"true\"}".getBytes()));
    when(s3Client.getObject(any())).thenReturn(s3Object);
    S3Helper s3Helper = new S3Helper(s3Client);
    ApiHelper helper = new ApiHelper();
    ObjectMapper mapper = ObjectMapperFactory.create(helper, s3Helper);
    MetadataResponse response = new MetadataResponse(
        helper.getApiName(DbExistsRequest.class, DbExistsResponse.class), true, "s3://mybucket/lambda/functions/spill", null);
    String payload = mapper.writeValueAsString(response);
    MetadataResponse result = mapper.readValue(payload, MetadataResponse.class);
    assertNotNull(result);
    assertEquals("dbExists", result.getApiName());
    assertTrue(result.isSpilled());
    assertNotNull(result.getSpillPath());
    assertNotNull(result.getApiResponse());
    assertTrue(result.getApiResponse() instanceof DbExistsResponse);
    DbExistsResponse resultResponse = (DbExistsResponse) result.getApiResponse();
    assertTrue(resultResponse.isExists());
  }

  @Test(expected = IOException.class)
  public void testMetadataResponseWithErrorMessage() throws IOException
  {
    ApiHelper helper = new ApiHelper();
    ObjectMapper mapper = ObjectMapperFactory.create(helper, mock(S3Helper.class));
    String payload = "{\"errorMessage\":\"org.apache.thrift.transport.TTransportException: Frame size (67108864) larger than max length (16384000)!\",\"errorType\":\"java.lang.RuntimeException\",\"stackTrace\":[\"com.amazonaws.athena.hms.ListPartitionsHandler.handleRequest(ListPartitionsHandler.java:34)\",\"sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\",\"sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\",\"sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\",\"java.lang.reflect.Method.invoke(Method.java:498)\"],\"cause\":{\"errorMessage\":\"Frame size (67108864) larger than max length (16384000)!\",\"errorType\":\"org.apache.thrift.transport.TTransportException\",\"stackTrace\":[\"org.apache.thrift.transport.TFastFramedTransport.readFrame(TFastFramedTransport.java:148)\",\"org.apache.thrift.transport.TFastFramedTransport.read(TFastFramedTransport.java:134)\",\"org.apache.thrift.transport.TTransport.readAll(TTransport.java:86)\",\"org.apache.thrift.protocol.TCompactProtocol.readByte(TCompactProtocol.java:634)\",\"org.apache.thrift.protocol.TCompactProtocol.readMessageBegin(TCompactProtocol.java:501)\",\"org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:77)\",\"org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.recv_get_partitions(ThriftHiveMetastore.java:2377)\",\"org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.get_partitions(ThriftHiveMetastore.java:2362)\",\"com.amazonaws.athena.hms.HMSClient.getPartitions(HMSClient.java:272)\",\"com.amazonaws.athena.hms.ListPartitionsHandler.handleRequest(ListPartitionsHandler.java:21)\",\"sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\",\"sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\",\"sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\",\"java.lang.reflect.Method.invoke(Method.java:498)\"]}}";
    mapper.readValue(payload, MetadataResponse.class);
  }
}
