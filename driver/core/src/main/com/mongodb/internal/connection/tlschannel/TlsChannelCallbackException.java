/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: MIT License, Copyright (c) [2015-2020] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel;

import javax.net.ssl.SSLException;

/**
 * Thrown during {@link TlsChannel} handshake to indicate that a user-supplied function threw an
 * exception.
 */
public class TlsChannelCallbackException extends SSLException {
  private static final long serialVersionUID = 8491908031320425318L;

  public TlsChannelCallbackException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
