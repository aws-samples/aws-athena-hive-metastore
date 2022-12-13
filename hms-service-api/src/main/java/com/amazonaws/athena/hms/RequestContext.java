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

import java.util.List;
import java.util.Map;

public class RequestContext
{
  private String id;
  private String principal;
  private String account;
  private String principalArn;
  private Map<String, String> principalTags;
  private List<String> iamGroups;

  public RequestContext()
  {
  }

  public RequestContext(String id, String principal, String account)
  {
    this.id = id;
    this.principal = principal;
    this.account = account;
  }

  public RequestContext(String id, String principal, String account, String principalArn,
                        Map<String, String> principalTags, List<String> iamGroups)
  {
    this.id = id;
    this.principal = principal;
    this.account = account;
    this.principalArn = principalArn;
    this.principalTags = principalTags;
    this.iamGroups = iamGroups;
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

  public String getPrincipalArn()
  {
    return principalArn;
  }

  public void setPrincipalArn(String principalArn)
  {
    this.principalArn = principalArn;
  }

  public Map<String, String> getPrincipalTags()
  {
    return principalTags;
  }

  public void setPrincipalTags(Map<String, String> principalTags)
  {
    this.principalTags = principalTags;
  }

  public List<String> getIamGroups()
  {
    return iamGroups;
  }

  public void setIamGroups(List<String> iamGroups)
  {
    this.iamGroups = iamGroups;
  }

  @Override
  public String toString()
  {
    return "{" +
        "id='" + id + '\'' +
        ", principal='" + principal + '\'' +
        ", account='" + account + '\'' +
        ", principalArn='" + principalArn + '\'' +
        ", principalTags='" + principalTags + '\'' +
        ", iamGroups='" + iamGroups + '\'' +
        '}';
  }
}
