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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configuration class that stores key/value pairs as strings similar to
 * the System properties object. The class includes helper methods to parse the
 * strings into numbers or booleans and it allows for loading a standard Java
 * properties file from disk. The  {@link loadSystemAndDefaults} will be called
 * to set defaults and optionally load values from the System properties list.
 * <p>
 * Note that multiple threads can read from this config object but modifying
 * values is not thread safe.
 * @since 1.7.0
 */
public class Config {
  private static final Logger LOG = LoggerFactory.getLogger(Config.class);

  /** Flag to determine if we're running under Windows or not */
  public static final boolean RUNNING_WINDOWS;
  static {
    // this should never be null, but who knows?
    RUNNING_WINDOWS = System.getProperty("os.name") != null ? System
        .getProperty("os.name").contains("Windows") : false;
  }

  /** How many times we should retry an RPC */
  private int max_retry_attempts;
  
  /** How often, in ms, to flush buffered RPC calls */
  private volatile short flush_interval;
  
  /** ?? */
  private short nsre_low_watermark;
  
  /** The maximum number of NSREs outstanding */
  private short nsre_high_watermark;
  
  /** How many buffered increment RPCs to maintain in memory at any given time */
  private volatile int increment_buffer_size;
  
  /**
   * The list of properties configured to their defaults or modified by users.
   * We use a non-synchronized hash map for fast access as opposed to the
   * HashTable backed properties objects.
   */
  protected final HashMap<String, String> properties = new HashMap<String, String>();

  /** Holds default values for the config */
  protected final HashMap<String, String> default_map = new HashMap<String, String>();

  /** Tracks the location of the file that was actually loaded */
  protected String config_location;

  /**
   * Constructor that initializes default configuration values from the
   * System.properties map.
   */
  public Config() {
    loadSystemAndDefaults();
  }

  /**
   * Constructor that initializes default values and attempts to load the given
   * properties file
   * @param file Path to the file to load
   * @throws IOException Thrown if unable to read or parse the file
   */
  public Config(final String file) throws IOException {
    loadSystemAndDefaults();
    loadConfig(file);
  }

  /**
   * Constructor for subclasses who want a copy of the parent config but without
   * the ability to modify the original values.
   * 
   * This constructor will not re-read the file, but it will copy the location
   * so if a child wants to reload the properties periodically, they may do so
   * @param parent Parent configuration object to load from
   */
  public Config(final Config parent) {
    properties.putAll(parent.properties);
    config_location = parent.config_location;
    loadSystemAndDefaults();
  }

  /**
   * Allows for modifying properties after loading
   * 
   * WARNING: This should only be used on initialization of the config object
   * and it is not thread safe.
   * 
   * @param property The name of the property to override
   * @param value The value to store
   */
  public void overrideConfig(final String property, final String value) {
    properties.put(property, value);
    setLocals();
  }

  /**
   * Returns the given property as a String
   * @param property The property to fetch
   * @return The property value as a string or null if the property did not exist
   */
  public final String getString(final String property) {
    return properties.get(property);
  }

  /**
   * Returns the given property as an integer
   * @param property The property to fetch
   * @return A parsed integer or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final int getInt(final String property) {
    return Integer.parseInt(properties.get(property));
  }

  /**
   * Returns the given property as a short
   * @param property The property to fetch
   * @return A parsed short or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final short getShort(final String property) {
    return Short.parseShort(properties.get(property));
  }

  /**
   * Returns the given property as a long
   * @param property The property to fetch
   * @return A parsed long or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final long getLong(final String property) {
    return Long.parseLong(properties.get(property));
  }

  /**
   * Returns the given property as a float
   * @param property The property to fetch
   * @return A parsed float or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final float getFloat(final String property) {
    return Float.parseFloat(properties.get(property));
  }

  /**
   * Returns the given property as a double
   * @param property The property to fetch
   * @return A parsed double or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final double getDouble(final String property) {
    return Double.parseDouble(properties.get(property));
  }

  /**
   * Returns the given property as a boolean
   * Property values are case insensitive and the following values will result
   * in a True return value: - 1 - True - Yes
   * Any other values, including an empty string, will result in a False
   * @param property The property to fetch
   * @return A parsed boolean
   * @throws NullPointerException if the property was not found
   */
  public final boolean getBoolean(final String property) {

    if (properties.containsKey(property)) {
      final String val = properties.get(property).toUpperCase();

      if (val.equals("1"))
        return true;
      if (val.equals("TRUE"))
        return true;
      if (val.equals("YES"))
        return true;
    }

    return false;
  }

  /**
   * Returns the directory name, making sure the end is an OS dependent slash
   * @param property The property to fetch
   * @return The property value with a forward or back slash appended
   * @throws NullPointerException if the property was not found
   */
  public final String getDirectoryName(final String property) {
    final String directory = properties.get(property);
    if (directory == null || directory.isEmpty()) {
      return null;
    }
    if (RUNNING_WINDOWS) {
      // Windows swings both ways. If a forward slash was already used,
      // we'll
      // add one at the end if missing. Otherwise use the windows default
      // of \
      if (directory.charAt(directory.length() - 1) == '\\'
          || directory.charAt(directory.length() - 1) == '/') {
        return directory;
      }
      if (directory.contains("/")) {
        return directory + "/";
      }
      return directory + "\\";
    }
    
    if (directory.contains("\\")) {
      throw new IllegalArgumentException(
          "Unix path names cannot contain a back slash");
    }
    if (directory.charAt(directory.length() - 1) == '/') {
      return directory;
    }
    return directory + "/";
  }

  /**
   * Determines if the given property is in the map
   * @param property The property to search for
   * @return True if the property exists and has a value, not an empty string
   */
  public final boolean hasProperty(final String property) {
    final String val = properties.get(property);
    if (val == null)
      return false;
    if (val.isEmpty())
      return false;
    return true;
  }

  /**
   * Returns a simple string with the configured properties for debugging. Note
   * that any property with the string "PASS" will be obfuscated to hide
   * passwords.
   * @return A string with information about the config
   */
  public final String dumpConfiguration() {
    if (properties.isEmpty()) {
      return "No configuration settings stored";
    }

    final StringBuilder response = new StringBuilder("Configuration:\n");
    response.append("File [" + this.config_location + "]\n");
    int line = 0;
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (line > 0) {
        response.append("\n");
      }
      response.append("Key [" + entry.getKey() + "]  Value [");
      if (entry.getKey().toUpperCase().contains("PASS")) {
        response.append("********");
      } else {
        response.append(entry.getValue());
      }
      response.append("]");
      line++;
    }
    return response.toString();
  }

  /** @return An copy of the configuration map */
  public final Map<String, String> getMap() {
    return new HashMap<String, String>(properties);
  }

  /**
   * Loads options from the JVM system properties and/or sets defaults
   */
  private void loadSystemAndDefaults() {
    default_map.put("hbase.rpcs.max_retry_attempts", "10");
    default_map.put("hbase.rpcs.buffered_flush_interval", "1000");
    default_map.put("hbase.region_client.check_channel_write_status", "false");
    default_map.put("hbase.nsre.low_watermark", "1000");
    default_map.put("hbase.nsre.high_watermark", "10000");
    
    /**
     * How many different counters do we want to keep in memory for buffering.
     * Each entry requires storing the table name, row key, family name and
     * column qualifier, plus 4 small objects.
     *
     * Assuming an average table name of 10 bytes, average key of 20 bytes,
     * average family name of 10 bytes and average qualifier of 8 bytes, this
     * would require 65535 * (10 + 20 + 10 + 8 + 4 * 32) / 1024 / 1024 = 11MB
     * of RAM, which isn't too excessive for a default value.  Of course this
     * might bite people with large keys or qualifiers, but then it's normal
     * to expect they'd tune this value to cater to their unusual requirements.
     */
    default_map.put("hbase.increments.buffer_size", "65535");
    
    default_map.put("hbase.zookeeper.quorum", "localhost");
    default_map.put("hbase.zookeeper.znode.parent", "/hbase");
    default_map.put("hbase.timer.tick", "20");
    
    default_map.put("hbase.security.auth.enable", "false");
    
    for (Map.Entry<String, String> entry : default_map.entrySet()) {
      if (!properties.containsKey(entry.getKey()))
        properties.put(entry.getKey(), entry.getValue());
    }
    
    setLocals();
  }

  /**
   * Sets the local fields that are accessed often enough that we want to avoid
   * hash map lookups.
   */
  private void setLocals() {
    max_retry_attempts = getInt("hbase.rpcs.max_retry_attempts");
    flush_interval = getShort("hbase.rpcs.buffered_flush_interval");
    nsre_low_watermark = getShort("hbase.nsre.low_watermark");
    nsre_high_watermark = getShort("hbase.nsre.high_watermark");
    increment_buffer_size = getInt("hbase.increments.buffer_size");
  }
  
  /**
   * Attempts to load the configuration from the given location
   * @param file Path to the file to load
   * @throws IOException Thrown if there was an issue reading the file
   * @throws FileNotFoundException Thrown if the config file was not found
   */
  protected void loadConfig(final String file) throws FileNotFoundException,
      IOException {
    final FileInputStream file_stream = new FileInputStream(file);
    try {
      final Properties props = new Properties();
      props.load(file_stream);
  
      // load the hash map
      loadHashMap(props);
  
      // no exceptions thrown, so save the valid path and exit
      LOG.info("Successfully loaded configuration file: " + file);
      config_location = file;
    } finally {
      file_stream.close();
    }
  }

  /**
   * Called from {@link #loadConfig} to copy the properties into the hash map.
   * @param props The loaded Properties object to copy
   */
  private void loadHashMap(final Properties props) {
    @SuppressWarnings("rawtypes")
    final Enumeration e = props.propertyNames();
    while (e.hasMoreElements()) {
      final String key = (String) e.nextElement();
      properties.put(key, props.getProperty(key));
    }
  }

  // GETTERS & SETTERS
  /** @return How many times we should retry an RPC */
  public int maxRetryAttempts() {
    return max_retry_attempts;
  }
  
  /** @return How often, in ms, to flush buffered RPC calls */
  public short flushInterval() {
    return flush_interval;
  }
  
  /** @return ?? */
  public short nsreLowWatermark() {
    return nsre_low_watermark;
  }
  
  /** @return The maximum number of NSREs outstanding */
  public short nsreHighWatermark() {
    return nsre_high_watermark;
  }
  
  /** @return How many buffered increment RPCs to maintain in memory at any 
   * given time */
  public int incrementBufferSize() {
    return increment_buffer_size;
  }
}