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

public class RequestContext
{
  private String id;
  private String principal;
  private String account;

  public RequestContext()
  {
  }

  public RequestContext(String id, String principal, String account)
  {
    this.id = id;
    this.principal = principal;
    this.account = account;
  }

  public String getId()
  {
    return id;
  }

  public void setId(String id)
  {
    this.id = id;
  }

  public String getPrincipal()
  {
    return principal;
  }

  public void setPrincipal(String principal)
  {
    this.principal = principal;
  }

  public String getAccount()
  {
    return account;
  }

  public void setAccount(String account)
  {
    this.account = account;
  }

  @Override
  public String toString()
  {
    return "{" +
        "id='" + id + '\'' +
        ", principal='" + principal + '\'' +
        ", account='" + account + '\'' +
        '}';
  }
}
