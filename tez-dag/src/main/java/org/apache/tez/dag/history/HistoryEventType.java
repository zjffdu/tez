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

package org.apache.tez.dag.history;

public enum HistoryEventType {
  APP_LAUNCHED,
  AM_LAUNCHED,
  AM_STARTED,
  DAG_SUBMITTED,
  DAG_INITIALIZED,
  DAG_STARTED,
  DAG_FINISHED,
  DAG_KILL_REQUEST,
  VERTEX_INITIALIZED,
  VERTEX_STARTED,
  VERTEX_CONFIGURE_DONE,
  VERTEX_FINISHED,
  TASK_STARTED,
  TASK_FINISHED,
  TASK_ATTEMPT_STARTED,
  TASK_ATTEMPT_FINISHED,
  CONTAINER_LAUNCHED,
  CONTAINER_STOPPED,
  DAG_COMMIT_STARTED,
  VERTEX_COMMIT_STARTED,
  VERTEX_GROUP_COMMIT_STARTED,
  VERTEX_GROUP_COMMIT_FINISHED,
  DAG_RECOVERED
}
