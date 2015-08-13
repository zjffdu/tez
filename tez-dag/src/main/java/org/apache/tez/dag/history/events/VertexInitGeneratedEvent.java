/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.dag.history.events;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.utils.TezEventUtils;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.TezEventProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexInitGeneratedEventProto;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class VertexInitGeneratedEvent implements HistoryEvent {

  private List<TezEvent> events;
  private TezVertexID vertexId;

  public VertexInitGeneratedEvent(TezVertexID vertexId,
                                               List<TezEvent> events) {
    this.vertexId = vertexId;
    Preconditions.checkArgument(events != null && events.size() > 0);
    this.events = events;
  }

  public VertexInitGeneratedEvent() {
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_INIT_GENERATED_EVENTS;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return false;
  }

  public VertexInitGeneratedEventProto toProto() throws IOException {
    List<TezEventProto> tezEventProtos = null;
    if (events != null) {
      tezEventProtos = Lists.newArrayListWithCapacity(events.size());
      for (TezEvent event : events) {
        tezEventProtos.add(TezEventUtils.toProto(event));
      }
    }
    return VertexInitGeneratedEventProto.newBuilder()
        .setVertexId(vertexId.toString())
        .addAllTezEvents(tezEventProtos)
        .build();
  }

  public void fromProto(VertexInitGeneratedEventProto proto) throws IOException {
    this.vertexId = TezVertexID.fromString(proto.getVertexId());
    int eventCount = proto.getTezEventsCount();
    if (eventCount > 0) {
      this.events = Lists.newArrayListWithCapacity(eventCount);
    }
    for (TezEventProto eventProto :
        proto.getTezEventsList()) {
      this.events.add(TezEventUtils.fromProto(eventProto));
    }
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    VertexInitGeneratedEventProto proto =
        VertexInitGeneratedEventProto.parseDelimitedFrom(inputStream);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertex_id=" + vertexId.toString()
        + ", eventCount=" + (events != null ? events.size() : "null");

  }

  public TezVertexID getVertexID() {
    return this.vertexId;
  }

  public List<TezEvent> getTezEvents() {
    return this.events;
  }

}
