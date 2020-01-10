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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestPaginator {
  @Test
  public void testEmptyPaginatorWithAllEntries() throws TException {
    StringPaginator paginator = new StringPaginator(new HashMap<>());
    PaginatedResponse<String> result = paginator.paginateByNames(null, (short) -1);
    assertNotNull(result);
    assertNull(result.getNextToken());
    assertNotNull(result.getEntries());
    assertTrue(result.getEntries().isEmpty());
  }

  @Test
  public void testEmptyPaginatorWithZeroEntry() throws TException {
    StringPaginator paginator = new StringPaginator(new HashMap<>());
    PaginatedResponse<String> result = paginator.paginateByNames(null, (short) 0);
    assertNotNull(result);
    assertNull(result.getNextToken());
    assertNotNull(result.getEntries());
    assertTrue(result.getEntries().isEmpty());
  }

  @Test
  public void testEmptyPaginatorWithPageEntry() throws TException {
    StringPaginator paginator = new StringPaginator(new HashMap<>());
    PaginatedResponse<String> result = paginator.paginateByNames(null, (short) 4);
    assertNotNull(result);
    assertNull(result.getNextToken());
    assertNotNull(result.getEntries());
    assertTrue(result.getEntries().isEmpty());
  }

  @Test
  public void testStringPaginatorWithAllEntries() throws TException {
    StringPaginator paginator = new StringPaginator(getData(6));
    PaginatedResponse<String> result = paginator.paginateByNames(null, (short) -1);
    assertNotNull(result);
    assertNull(result.getNextToken());
    assertNotNull(result.getEntries());
    assertEquals(6, result.getEntries().size());
  }

  @Test
  public void testStringPaginatorWithZeroEntry() throws TException {
    StringPaginator paginator = new StringPaginator(getData(6));
    PaginatedResponse<String> result = paginator.paginateByNames(null, (short) 0);
    assertNotNull(result);
    assertNull(result.getNextToken());
    assertNotNull(result.getEntries());
    assertEquals(0, result.getEntries().size());
    result = paginator.paginateByNames(Paginator.encrypt("k2"), (short) 0);
    assertNotNull(result);
    assertEquals(Paginator.encrypt("k2"), result.getNextToken());
    assertNotNull(result.getEntries());
    assertEquals(0, result.getEntries().size());
  }

  @Test
  public void testStringPaginatorWithPageEntry() throws TException {
    StringPaginator paginator = new StringPaginator(getData(9));
    PaginatedResponse<String> result = paginator.paginateByNames(null, (short) 4);
    assertNotNull(result);
    assertEquals(Paginator.encrypt("k4"), result.getNextToken());
    assertNotNull(result.getEntries());
    List<String> list = result.getEntries();
    assertEquals(4, list.size());
    for (int i = 0; i < 4; i++) {
      assertEquals("v" + i, list.get(i));
    }
    result = paginator.paginateByNames(result.getNextToken(), (short) 4);
    assertNotNull(result);
    assertEquals(Paginator.encrypt("k8"), result.getNextToken());
    assertNotNull(result.getEntries());
    list = result.getEntries();
    assertEquals(4, list.size());
    for (int i = 0; i < 4; i++) {
      assertEquals("v" + (i + 4), list.get(i));
    }
    result = paginator.paginateByNames(result.getNextToken(), (short) 4);
    assertNotNull(result);
    assertNull(result.getNextToken());
    assertNotNull(result.getEntries());
    list = result.getEntries();
    assertEquals(1, list.size());
    assertEquals("v8", list.get(0));
  }

  @Test
  public void testGetAllEntriesWithFullPages() throws TException {
    StringPaginator paginator = new StringPaginator(getData(16));
    Set<String> resultSet = new HashSet<>();
    PaginatedResponse<String> result;
    String nextToken = null;
    do {
      result = paginator.paginateByNames(nextToken, (short) 4);
      if (result != null && result.getEntries() != null && !result.getEntries().isEmpty()) {
        resultSet.addAll(result.getEntries());
      }
      nextToken = result.getNextToken();
    } while (nextToken != null);
    assertEquals(16, resultSet.size());
    for (int i = 0; i < 16; i++) {
      assertTrue(resultSet.contains("v" + i));
    }
  }

  @Test
  public void testGetAllEntriesWithoutFullPages() throws TException {
    StringPaginator paginator = new StringPaginator(getData(19));
    Set<String> resultSet = new HashSet<>();
    PaginatedResponse<String> result;
    String nextToken = null;
    do {
      result = paginator.paginateByNames(nextToken, (short) 5);
      if (result != null && result.getEntries() != null && !result.getEntries().isEmpty()) {
        resultSet.addAll(result.getEntries());
      }
      nextToken = result.getNextToken();
    } while (nextToken != null);
    assertEquals(19, resultSet.size());
    for (int i = 0; i < 19; i++) {
      assertTrue(resultSet.contains("v" + i));
    }
  }

  @Test
  public void testEncrypDecrypt() {
    String original = "";
    String encrypted = Paginator.encrypt(original);
    assertEquals(original, Paginator.decrypt(encrypted));
    original = "my@Test-_name";
    encrypted = Paginator.encrypt(original);
    assertEquals(original, Paginator.decrypt(encrypted));
  }

  private Map<String, String> getData(int num) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < num; i++) {
      map.put("k" + i, "v" + i);
    }
    return map;
  }

  static class StringPaginator extends Paginator<String> {
    private final Map<String, String> map;

    public StringPaginator(Map<String, String> map) {
      this.map = map;
    }

    @Override
    protected Collection<String> getNames() throws TException {
      return map.keySet();
    }

    @Override
    protected List<String> getEntriesByNames(List<String> names) throws TException {
      List<String> list = new ArrayList<>();
      if (names != null && !names.isEmpty()) {
        for (String name : names) {
          list.add(map.get(name));
        }
      }

      return list;
    }
  }
}
