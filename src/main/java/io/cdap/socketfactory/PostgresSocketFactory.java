/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.socketfactory;

import com.google.cloud.sql.core.CoreSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.SocketFactory;

/**
 * A Postgres {@link SocketFactory} that keeps track of the number of bytes written.
 */
public class PostgresSocketFactory extends SocketFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlSocketFactory.class);
  private static final String DEPRECATED_SOCKET_ARG = "SocketFactoryArg";
  private static final String POSTGRES_SUFFIX = "/.s.PGSQL.5432";
  private static String delegateClass;
  private static AtomicLong bytesWritten = new AtomicLong(0);

  private Properties props;

  static {
    CoreSocketFactory.addArtifactId("postgres-socket-factory");
  }

  public static void setDelegateClass(String name) {
    delegateClass = name;
  }

  public static long getBytesWritten() {
    return bytesWritten.get();
  }


  /**
   * Implements the {@link SocketFactory} constructor, which can be used to create authenticated
   * connections to a Cloud SQL instance.
   */
  public PostgresSocketFactory(Properties info) {
    String oldInstanceKey = info.getProperty(DEPRECATED_SOCKET_ARG);
    if (oldInstanceKey != null) {
      LOG.warn(
        String.format(
          "The '%s' property has been deprecated. Please update your postgres driver and use"
            + "the  '%s' property in your JDBC url instead.",
          DEPRECATED_SOCKET_ARG, CoreSocketFactory.CLOUD_SQL_INSTANCE_PROPERTY));
      info.setProperty(CoreSocketFactory.CLOUD_SQL_INSTANCE_PROPERTY, oldInstanceKey);
    }
    this.props = info;
  }

  @Deprecated
  public PostgresSocketFactory(String instanceName) {
    // Deprecated constructor for converting 'SocketFactoryArg' to 'CloudSqlInstance'
    this(createDefaultProperties(instanceName));
  }

  private static Properties createDefaultProperties(String instanceName) {
    Properties info = new Properties();
    info.setProperty(DEPRECATED_SOCKET_ARG, instanceName);
    return info;
  }

  @Override
  public Socket createSocket() throws IOException {
    try {
      com.google.cloud.sql.mysql.SocketFactory fac = (com.google.cloud.sql.mysql.SocketFactory) Class.forName(delegateClass).newInstance();
      Socket delegate = CoreSocketFactory.connect(props, POSTGRES_SUFFIX);
      return new BytesTrackingSocket(delegate, bytesWritten);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new IOException(String.format("Could not instantiate class %s", delegateClass), e);
    }
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    Socket delegate = javax.net.SocketFactory.getDefault().createSocket(host, port);
    return new BytesTrackingSocket(delegate, bytesWritten);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
    throws IOException {
    Socket delegate = javax.net.SocketFactory.getDefault().createSocket(host, port, localHost, localPort);
    return new BytesTrackingSocket(delegate, bytesWritten);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    Socket delegate = javax.net.SocketFactory.getDefault().createSocket(host, port);
    return new BytesTrackingSocket(delegate, bytesWritten);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
    throws IOException {
    Socket delegate = javax.net.SocketFactory.getDefault().createSocket(address, port, localAddress, localPort);
    return new BytesTrackingSocket(delegate, bytesWritten);
  }

}
