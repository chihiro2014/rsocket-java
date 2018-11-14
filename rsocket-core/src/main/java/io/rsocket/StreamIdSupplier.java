/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

final class StreamIdSupplier {
  private static final AtomicIntegerFieldUpdater<StreamIdSupplier> STREAM_ID =
      AtomicIntegerFieldUpdater.newUpdater(StreamIdSupplier.class, "streamId");
  private volatile int streamId;
  
  private StreamIdSupplier(int streamId) {
    this.streamId = streamId;
  }
  
  static StreamIdSupplier clientSupplier() {
    return new StreamIdSupplier(-1);
  }
  
  static StreamIdSupplier serverSupplier() {
    return new StreamIdSupplier(0);
  }
  
  int nextStreamId() {
   return STREAM_ID.addAndGet(this, 2);
  }
  
  boolean isBeforeOrCurrent(int streamId) {
    return this.streamId >= streamId && streamId > 0;
  }
}
