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

import java.util.Map;

public class CreateDatabaseRequest extends ApiRequest
{
  private String name;
  private String description;
  private String location;
  private Map<String, String> params;

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public String getDescription()
  {
    return description;
  }

  public void setDescription(String description)
  {
    this.description = description;
  }

  public String getLocation()
  {
    return location;
  }

  public void setLocation(String location)
  {
    this.location = location;
  }

  public Map<String, String> getParams()
  {
    return params;
  }

  public void setParams(Map<String, String> params)
  {
    this.params = params;
  }

  public CreateDatabaseRequest withName(String name)
  {
    this.name = name;
    return this;
  }

  public CreateDatabaseRequest withDescription(String description)
  {
    this.description = description;
    return this;
  }

  public CreateDatabaseRequest withLocation(String location)
  {
    this.location = location;
    return this;
  }

  public CreateDatabaseRequest withParams(Map<String, String> params)
  {
    this.params = params;
    return this;
  }
}
