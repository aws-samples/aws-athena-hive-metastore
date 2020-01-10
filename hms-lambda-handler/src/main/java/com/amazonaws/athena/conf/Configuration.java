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
package com.amazonaws.athena.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration
{
  private final Properties properties;

  public Configuration()
  {
    this(new Properties());
  }

  public Configuration(Properties properties)
  {
    this.properties = properties;
  }

  public static Configuration fromStream(InputStream inputStream) throws IOException
  {
    Configuration configuration = new Configuration();
    configuration.properties.load(inputStream);
    return configuration;
  }

  public static Configuration fromFile(File filePath) throws IOException
  {
    try (InputStream inputStream = new FileInputStream(filePath)) {
      return fromStream(inputStream);
    }
  }

  public static Configuration loadDefaultFromFile(File propertyFile)
  {
    try {
      return fromFile(propertyFile);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Configuration loadDefaultFromClasspath(String propertyFileName)
  {
    try (InputStream inputStream = Configuration.class.getResourceAsStream("/" + propertyFileName)) {
      return fromStream(inputStream);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getProperty(String name)
  {
    return (String) properties.get(name);
  }

  public void setProperty(String name, String value)
  {
    properties.put(name, value);
  }

  public String getString(String name, String deflt)
  {
    String value = properties.getProperty(name);
    return (value != null) ? value : deflt;
  }

  public long getLong(String name, long deflt)
  {
    String value = properties.getProperty(name);
    return (value != null) ? Long.parseLong(value) : deflt;
  }

  public int getInt(String name, int deflt)
  {
    String value = properties.getProperty(name);
    return (value != null) ? Integer.parseInt(value) : deflt;
  }

  public double getDouble(String name, double deflt)
  {
    String value = properties.getProperty(name);
    return (value != null) ? Double.parseDouble(value) : deflt;
  }

  public boolean getBoolean(String name, boolean deflt)
  {
    String value = properties.getProperty(name);
    return (value != null) ? Boolean.parseBoolean(value) : deflt;
  }
}
