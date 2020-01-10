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

import org.apache.thrift.TException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class Paginator<T>
{
  protected abstract Collection<String> getNames() throws TException;
  protected abstract List<T> getEntriesByNames(List<String> names) throws TException;

  public PaginatedResponse<T> paginateByNames(String token, short maxSize) throws TException
  {
    Collection<String> names = getNames();
    String nextToken;
    List<T> list;
    if (names != null && !names.isEmpty()) {
      // sort names by nature order
      List<String> sortedNames = new ArrayList<String>(names);
      Collections.sort(sortedNames);
      if (maxSize == 0) {
        list = new ArrayList<>();
        // point to the same next position
        nextToken = token;
      }
      else if (maxSize > 0) {
        // first check the start index
        int startIndex;
        // normal page size
        if (token == null) {
          // first page
          startIndex = 0;
        }
        else {
          // not first page
          // check the index of the token, i.e., the first entry in the new page
          startIndex = sortedNames.indexOf(decrypt(token));
          if (startIndex == -1) {
            throw new RuntimeException("Failed to find token " + token);
          }
        }

        // number of entries left
        int num = sortedNames.size() - startIndex;
        if (num <= maxSize) {
          // only one page left
          List<String> pageNames = sortedNames.subList(startIndex, sortedNames.size());
          list = getEntriesByNames(pageNames);
          // no more entries
          nextToken = null;
        }
        else {
          // more than one pages left
          List<String> pageNames = sortedNames.subList(startIndex, startIndex + maxSize);
          list = getEntriesByNames(pageNames);
          // use the first name in the next page as the nextToken
          nextToken = encrypt(sortedNames.get(startIndex + maxSize));
        }
      }
      else {
        // -1 or negative page size means to fetch all data without actual pagination
        if (token == null) {
          list = getEntriesByNames(sortedNames);
        }
        else {
          // check the index of the token, i.e., the first entry in the new page
          int index = sortedNames.indexOf(decrypt(token));
          if (index == -1) {
            throw new RuntimeException("Failed to find token " + token);
          }
          // fetch all remaining entries
          List<String> pageNames = sortedNames.subList(index, sortedNames.size());
          list = getEntriesByNames(pageNames);
        }
        // fetched all and no more entry left
        nextToken = null;
      }
    }
    else {
      list = new ArrayList<>();
      nextToken = null;
    }

    return new PaginatedResponse<T>(list, nextToken);
  }

  public static String encrypt(String original)
  {
    if (original == null) {
      return original;
    }

    return Base64.getEncoder().encodeToString(original.getBytes(StandardCharsets.UTF_8));
  }

  public static String decrypt(String original)
  {
    if (original == null) {
      return null;
    }

    return new String(Base64.getDecoder().decode(original), StandardCharsets.UTF_8);
  }
}
