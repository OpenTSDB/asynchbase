/*
 * Copyright (C) 2015  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Config.class })
public class TestConfig {

  @Test
  public void defaultCtor() throws Exception {
    final Config config = new Config();
    assertNotNull(config);
    assertNull(config.config_location);
  }

  @Test
  public void constructorChild() throws Exception {
    final Config config = new Config();
    assertNotNull(config);
    assertNull(config.config_location);
    final Config child = new Config(config);
    assertNotNull(child);
    assertNull(child.config_location);
    assertTrue(config.getMap() != child.getMap());
  }

  @Test
  public void constructorChildCopy() throws Exception {
    final Config config = new Config();
    assertNotNull(config);
    assertNull(config.config_location);
    final Config child = new Config(config);
    assertNotNull(child);
    assertNull(child.config_location);
    child.overrideConfig("hbase.zookeeper.znode.parent", "/myhbase");
    assertEquals("/hbase", config.getString("hbase.zookeeper.znode.parent"));
    assertEquals("/myhbase", child.getString("hbase.zookeeper.znode.parent"));
    assertTrue(config.getMap() != child.getMap());
  }

  @Test(expected = NullPointerException.class)
  public void constructorNullChild() throws Exception {
    new Config((Config) null);
  }

  @Test
  public void constructorWithFile() throws Exception {
    PowerMockito.whenNew(FileInputStream.class).withAnyArguments()
      .thenReturn(mock(FileInputStream.class));
    final Properties props = new Properties();
    props.setProperty("asynchbase.test", "val1");
    PowerMockito.whenNew(Properties.class).withNoArguments().thenReturn(props);
    
    final Config config = new Config("/tmp/config.file");
    assertNotNull(config);
    assertEquals("/tmp/config.file", config.config_location);
    assertEquals("val1", config.getString("asynchbase.test"));
  }

  @Test(expected = FileNotFoundException.class)
  public void constructorFileNotFound() throws Exception {
    new Config("/tmp/filedoesnotexist.conf");
  }

  @Test(expected = NullPointerException.class)
  public void constructorNullFile() throws Exception {
    new Config((String) null);
  }

  @Test(expected = FileNotFoundException.class)
  public void constructorEmptyFile() throws Exception {
    new Config("");
  }

  @Test(expected = FileNotFoundException.class)
  public void loadConfigNotFound() throws Exception {
    final Config config = new Config();
    config.loadConfig("/tmp/filedoesnotexist.conf");
  }

  @Test(expected = NullPointerException.class)
  public void loadConfigNull() throws Exception {
    final Config config = new Config();
    config.loadConfig(null);
  }

  @Test(expected = FileNotFoundException.class)
  public void loadConfigEmpty() throws Exception {
    final Config config = new Config();
    config.loadConfig("");
  }

  @Test
  public void overrideConfig() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.zk.base_path", "/myhbase");
    assertEquals("/myhbase", config.getString("asynchbase.zk.base_path"));
  }

  @Test
  public void getString() throws Exception {
    final Config config = new Config();
    assertEquals("/hbase", config.getString("hbase.zookeeper.znode.parent"));
  }

  @Test
  public void getStringNull() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.null", null);
    assertNull(config.getString("asynchbase.null"));
  }

  @Test
  public void getStringDoesNotExist() throws Exception {
    final Config config = new Config();
    assertNull(config.getString("asynchbase.nosuchkey"));
  }

  @Test
  public void getInt() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.int", 
        Integer.toString(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, 
        config.getInt("asynchbase.int"));
  }

  @Test
  public void getIntNegative() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.int", 
        Integer.toString(Integer.MIN_VALUE));
    assertEquals(Integer.MIN_VALUE, 
        config.getInt("asynchbase.int"));
  }

  @Test(expected = NumberFormatException.class)
  public void getIntNull() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.null", null);
    config.getInt("asynchbase.null");
  }

  @Test(expected = NumberFormatException.class)
  public void getIntDoesNotExist() throws Exception {
    final Config config = new Config();
    config.getInt("asynchbase.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getIntNFE() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.int", 
        "this can't be parsed to int");
    config.getInt("asynchbase.int");
  }

  @Test
  public void getShort() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.short", 
        Short.toString(Short.MAX_VALUE));
    assertEquals(Short.MAX_VALUE, 
        config.getShort("asynchbase.short"));
  }

  @Test
  public void getShortNegative() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.short", 
        Short.toString(Short.MIN_VALUE));
    assertEquals(Short.MIN_VALUE, 
        config.getShort("asynchbase.short"));
  }

  @Test(expected = NumberFormatException.class)
  public void getShortNull() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.null", null);
    config.getShort("asynchbase.null");
  }

  @Test(expected = NumberFormatException.class)
  public void getShortDoesNotExist() throws Exception {
    final Config config = new Config();
    config.getShort("asynchbase.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getShortNFE() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.short", 
        "this can't be parsed to short");
    config.getShort("asynchbase.short");
  }

  @Test
  public void getLong() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.long", Long.toString(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, config.getLong("asynchbase.long"));
  }

  @Test
  public void getLongNegative() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.long", Long.toString(Long.MIN_VALUE));
    assertEquals(Long.MIN_VALUE, 
        config.getLong("asynchbase.long"));
  }

  @Test(expected = NumberFormatException.class)
  public void getLongNull() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.null", null);
    config.getLong("asynchbase.null");
  }

  @Test(expected = NumberFormatException.class)
  public void getLongDoesNotExist() throws Exception {
    final Config config = new Config();
    config.getLong("asynchbase.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getLongNullNFE() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.long", "this can't be parsed to long");
    config.getLong("asynchbase.long");
  }

  @Test
  public void getFloat() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.float", Float.toString(Float.MAX_VALUE));
    assertEquals(Float.MAX_VALUE, 
        config.getFloat("asynchbase.float"), 0.000001);
  }

  @Test
  public void getFloatNegative() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.float", Float.toString(Float.MIN_VALUE));
    assertEquals(Float.MIN_VALUE, 
        config.getFloat("asynchbase.float"), 0.000001);
  }

  @Test
  public void getFloatNaN() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.float", "NaN");
    assertEquals(Float.NaN, 
        config.getDouble("asynchbase.float"), 0.000001);
  }

  @Test(expected = NumberFormatException.class)
  public void getFloatNaNBadCase() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.float", "nan");
    assertEquals(Float.NaN, 
        config.getDouble("asynchbase.float"), 0.000001);
  }

  @Test
  public void getFloatPIfinity() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.float", "Infinity");
    assertEquals(Float.POSITIVE_INFINITY, 
        config.getDouble("asynchbase.float"), 0.000001);
  }

  @Test
  public void getFloatNIfinity() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.float", "-Infinity");
    assertEquals(Float.NEGATIVE_INFINITY, 
        config.getDouble("asynchbase.float"), 0.000001);
  }

  @Test(expected = NullPointerException.class)
  public void getFloatNull() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.null", null);
    config.getFloat("asynchbase.null");
  }

  @Test(expected = NullPointerException.class)
  public void getFloatDoesNotExist() throws Exception {
    final Config config = new Config();
    config.getFloat("asynchbase.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getFloatNFE() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.float", "this can't be parsed to float");
    config.getFloat("asynchbase.float");
  }

  @Test
  public void getDouble() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.double", Double.toString(Double.MAX_VALUE));
    assertEquals(Double.MAX_VALUE, 
        config.getDouble("asynchbase.double"), 0.000001);
  }

  @Test
  public void getDoubleNegative() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.double", Double.toString(Double.MIN_VALUE));
    assertEquals(Double.MIN_VALUE, 
        config.getDouble("asynchbase.double"), 0.000001);
  }

  @Test
  public void getDoubleNaN() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.double", "NaN");
    assertEquals(Double.NaN, 
        config.getDouble("asynchbase.double"), 0.000001);
  }

  @Test(expected = NumberFormatException.class)
  public void getDoubleNaNBadCase() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.double", "nan");
    assertEquals(Double.NaN, 
        config.getDouble("asynchbase.double"), 0.000001);
  }

  @Test
  public void getDoublePIfinity() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.double", "Infinity");
    assertEquals(Double.POSITIVE_INFINITY,
        config.getDouble("asynchbase.double"), 0.000001);
  }

  @Test
  public void getDoubleNIfinity() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.double", "-Infinity");
    assertEquals(Double.NEGATIVE_INFINITY,
        config.getDouble("asynchbase.double"), 0.000001);
  }

  @Test(expected = NullPointerException.class)
  public void getDoubleNull() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.null", null);
    config.getDouble("asynchbase.null");
  }

  @Test(expected = NullPointerException.class)
  public void getDoubleDoesNotExist() throws Exception {
    final Config config = new Config();
    config.getDouble("asynchbase.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getDoubleNFE() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.double", 
        "this can't be parsed to double");
    config.getDouble("asynchbase.double");
  }

  @Test
  public void getBool() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "true");
    assertTrue(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBool1() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "1");
    assertTrue(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBoolTrueCaseInsensitive() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "TrUe");
    assertTrue(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBoolYes() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "yes");
    assertTrue(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBoolYesCaseInsensitive() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "YeS");
    assertTrue(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBoolFalse() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "false");
    assertFalse(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBoolFalse0() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "0");
    assertFalse(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBoolFalse2() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "2");
    assertFalse(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBoolFalseNo() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "no");
    assertFalse(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getBoolFalseEmpty() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "");
    assertFalse(config.getBoolean("asynchbase.bool"));
  }

  @Test(expected = NullPointerException.class)
  public void getBoolFalseNull() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.null", null);
    config.getBoolean("asynchbase.null");
  }

  @Test
  public void getBoolFalseDoesNotExist() throws Exception {
    final Config config = new Config();
    assertFalse(config.getBoolean("asynchbase.nosuchkey"));
  }

  @Test
  public void getBoolFalseOther() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.bool", "blarg");
    assertFalse(config.getBoolean("asynchbase.bool"));
  }

  @Test
  public void getDirectoryNameAddSlash() throws Exception {
    // same for Windows && Unix
    final Config config = new Config();
    config.overrideConfig("asynchbase.dir", "/my/dir/");
    assertEquals("/my/dir/", config.getDirectoryName("asynchbase.dir"));
  }

  @Test
  public void getDirectoryNameHasSlash() throws Exception {
    // same for Windows && Unix
    final Config config = new Config();
    config.overrideConfig("asynchbase.dir", "/my/dir/");
    assertEquals("/my/dir/", config.getDirectoryName("asynchbase.dir"));
  }

  @Test
  public void getDirectoryNameWindowsAddSlash() throws Exception {
    if (Config.RUNNING_WINDOWS) {
      final Config config = new Config();
      config.overrideConfig("asynchbase.dir", "C:\\my\\dir");
      assertEquals("C:\\my\\dir\\", config.getDirectoryName("asynchbase.dir"));
    } else {
      assertTrue(true);
    }
  }

  @Test
  public void getDirectoryNameWindowsHasSlash() throws Exception {
    if (Config.RUNNING_WINDOWS) {
      final Config config = new Config();
      config.overrideConfig("asynchbase.dir", "C:\\my\\dir\\");
      assertEquals("C:\\my\\dir\\", config.getDirectoryName("asynchbase.dir"));
    } else {
      assertTrue(true);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void getDirectoryNameWindowsOnLinuxException() throws Exception {
    if (Config.RUNNING_WINDOWS) {
      throw new IllegalArgumentException("Can't run this on Windows");
    } else {
      final Config config = new Config();
      config.overrideConfig("asynchbase.dir", "C:\\my\\dir");
      config.getDirectoryName("asynchbase.dir");
    }
  }

  @Test
  public void getDirectoryNameNull() throws Exception {
    final Config config = new Config();
    assertNull(config.getDirectoryName("zookeeper.null"));
  }

  @Test
  public void hasProperty() throws Exception {
    final Config config = new Config();
    assertTrue(config.hasProperty("hbase.rpcs.buffered_flush_interval"));
  }

  @Test
  public void hasPropertyNull() throws Exception {
    final Config config = new Config();
    config.overrideConfig("asynchbase.null", null);
    assertFalse(config.hasProperty("asynchbase.null"));
  }

  @Test
  public void hasPropertyNot() throws Exception {
    final Config config = new Config();
    assertFalse(config.hasProperty("asynchbase.nosuchkey"));
  }
}