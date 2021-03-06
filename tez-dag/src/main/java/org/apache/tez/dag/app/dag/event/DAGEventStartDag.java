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

package org.apache.tez.dag.app.dag.event;

import java.net.URL;
import java.util.List;

import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.records.TezDAGID;

public class DAGEventStartDag extends DAGEvent {

  private final List<URL> additionalUrlsForClasspath;
  private final DAGStartedEvent dagStartedEvent;

  public DAGEventStartDag(TezDAGID dagId, List<URL> additionalUrlsForClasspath) {
    super(dagId, DAGEventType.DAG_START);
    this.additionalUrlsForClasspath = additionalUrlsForClasspath;
    this.dagStartedEvent = null;
  }
  
  public DAGEventStartDag(TezDAGID dagId, List<URL> additionalUrlsForClasspath, DAGStartedEvent dagStarteEvent) {
    super(dagId, DAGEventType.DAG_START);
    this.additionalUrlsForClasspath = additionalUrlsForClasspath;
    this.dagStartedEvent = dagStarteEvent;
  }

  public List<URL> getAdditionalUrlsForClasspath() {
    return this.additionalUrlsForClasspath;
  }
  
  public DAGStartedEvent getDagStartedEvent() {
    return dagStartedEvent;
  }
}