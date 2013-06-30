package org.hbase.async;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 *
 * Used to retrieve the default realm which is JVM specific.
 *
 * This class was culled from zookeeper which was culled from hadoop with some small changes.
 */
public class KerberosUtil {

  public static String getDefaultRealm()
      throws ClassNotFoundException, NoSuchMethodException,
      IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    Object kerbConf;
    Class<?> classRef;
    Method getInstanceMethod;
    Method getDefaultRealmMethod;
    if (System.getProperty("java.vendor").contains("IBM")) {
      classRef = Class.forName("com.ibm.security.krb5.internal.Config");
    } else {
      classRef = Class.forName("sun.security.krb5.Config");
    }
    getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
    kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
    getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm",
         new Class[0]);
    return (String)getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
  }
}
