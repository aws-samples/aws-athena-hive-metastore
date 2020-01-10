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
package com.amazonaws.athena.hms;

public class MetadataResponse implements AutoCloseable
{
  public static final String API_NAME = "apiName";
  public static final String IS_SPILLED = "spilled";
  public static final String SPILL_PATH = "spillPath";
  public static final String API_RESPONSE = "apiResponse";

  private final String apiName;

  // whether the API response object is spilled to S3 due to its size or not
  // if this flag is true, the client should read the response object from the
  // s3 path specified by the spillPath variable, otherwise, the response
  // is included in the apiResponse variable
  private final boolean spilled;

  // the s3 path that the spilled response object stored in S3
  private final String spillPath;

  private final ApiResponse apiResponse;

  public MetadataResponse(String apiName, boolean spilled, String spillPath, ApiResponse apiResponse)
  {
    this.apiName = apiName;
    this.spilled = spilled;
    this.spillPath = spillPath;
    this.apiResponse = apiResponse;
  }

  public String getApiName()
  {
    return apiName;
  }

  public boolean isSpilled()
  {
    return spilled;
  }

  public String getSpillPath()
  {
    return spillPath;
  }

  public ApiResponse getApiResponse()
  {
    return apiResponse;
  }

  @Override
  public void close() throws Exception
  {
  }
}
