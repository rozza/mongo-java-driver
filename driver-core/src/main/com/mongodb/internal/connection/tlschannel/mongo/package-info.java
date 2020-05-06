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

/**
 * This package contains the adaptation classes used so this library can work with ByteBufs and free used resources correctly.
 *
 * The changes are as follows:
 *
 *   - AsynchronousTlsChannelAdapter - originally from this package the AsynchronousTlsChannel was moved out to support other usecases, this
 *                                     is a simple proxy class, used by the TlsChannelStreamFactoryFactory.
 *   - ByteBufAllocator - This replaces the tls-channels BufferAllocator class, so that we can free any ByteBufs once used.
 *   - ByteBufHolder - This replaces the tls-channels BufferHolder class, and manages the ByteBuf correctly so it can be freed once used
 *   - TrackingByteBufAllocator - This replaces the tls-channels TrackingBufferAllocator class and is a tracking proxy for the Allocator class.
 *
 *  The tls-channel library is then updated to use the new counter parts instead of the originals. Any changes to the originals should be
 *  included in these classes.
 *
 *  The final change required is to update the logger code
 */
package com.mongodb.internal.connection.tlschannel.mongo;
