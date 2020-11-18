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

import com.google.common.base.Strings;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A MySQL {@link SocketFactory} that keeps track of the number of bytes written.
 */
public class SocketFactory implements com.mysql.cj.protocol.SocketFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SocketFactory.class);
  private static String delegateClass;
  private static AtomicLong bytesWritten;
  private Socket socket;

  public static void setDelegateClass(String name) {
    delegateClass = name;
  }

  public static long getBytesWritten() {
    return bytesWritten.get();
  }


  @Override
  public <T extends Closeable> T connect(
      String host, int portNumber, PropertySet props, int loginTimeout) throws IOException {
    return connect(host, portNumber, props.exposeAsProperties(), loginTimeout);
  }

  /**
   * Implements the interface for com.mysql.cj.protocol.SocketFactory for mysql-connector-java prior
   * to version 8.0.13. This change is required for backwards compatibility.
   */
  public <T extends Closeable> T connect(
      String hostname, int portNumber, Properties props, int loginTimeout) throws IOException {
    LOG.error("In connect");
    if (Strings.isNullOrEmpty(delegateClass)) {
      Socket delegate = javax.net.SocketFactory.getDefault().createSocket(hostname, portNumber);
      socket = new BytesTrackingSocket(delegate, bytesWritten);
    } else {
      try {
        javax.net.SocketFactory fac = (javax.net.SocketFactory) Class.forName(delegateClass).newInstance();
        Socket delegate = fac.createSocket(hostname, portNumber);
        socket = new BytesTrackingSocket(delegate, bytesWritten);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new IOException(String.format("Could not instantiate class %s", delegateClass), e);
      }
    }
    return socket;
  }
  // Cloud SQL sockets always use TLS and the socket returned by connect above is already TLS-ready.
  // It is fine to implement these as no-ops.
  @Override
  public void beforeHandshake() {
    LOG.error("In beforeHandshake");
  }

  @Override
  public <T extends Closeable> T performTlsHandshake(
      SocketConnection socketConnection, ServerSession serverSession) throws IOException {
    LOG.error("In performTlsHandshake");
    @SuppressWarnings("unchecked")
    T socket = (T) socketConnection.getMysqlSocket();
    return socket;
  }

  @Override
  public void afterHandshake() {
    LOG.error("In afterHandshake");
  }
}
