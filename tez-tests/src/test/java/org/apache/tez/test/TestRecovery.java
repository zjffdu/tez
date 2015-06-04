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
package org.apache.tez.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.conn.HttpInetSocketAddress;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.impl.VertexStats;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexRecoverableEventsGeneratedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.test.TestRecovery.ShutdownCondition.TIMING;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestRecovery {

  private static final Logger LOG = LoggerFactory.getLogger(TestRecovery.class);

  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster = null;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestRecovery.class.getName() + "-tmpDir";
  private static MiniDFSCluster dfsCluster = null;
  private static TezClient tezSession = null;
  private static FileSystem remoteFs = null;
  private static TezConfiguration tezConf = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    LOG.info("Starting mini clusters");
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
          .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TestRecovery.class.getName(), 1, 1, 1);
      Configuration miniTezconf = new Configuration(conf);
      miniTezconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 4);
      miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      miniTezCluster.init(miniTezconf);
      miniTezCluster.start();
    }
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    if (tezSession != null) {
      try {
        LOG.info("Stopping Tez Session");
        tezSession.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (miniTezCluster != null) {
      try {
        LOG.info("Stopping MiniTezCluster");
        miniTezCluster.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (dfsCluster != null) {
      try {
        LOG.info("Stopping DFSCluster");
        dfsCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static class ShutdownCondition {

    public static enum TIMING {
      PRE, // before the event
      POST, // after the event
    }

    private TIMING timing;
    private HistoryEvent event;

    public ShutdownCondition(TIMING timing, HistoryEvent event) {
      this.timing = timing;
      this.event = event;
    }

    public ShutdownCondition() {
    }

    private String encodeHistoryEvent(HistoryEvent event) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      event.toProtoStream(out);
      return event.getClass().getName() + ","
          + Base64.encodeBase64String(out.toByteArray());
    }

    private HistoryEvent decodeHistoryEvent(String eventClass, String base64)
        throws IOException {
      ByteArrayInputStream in = new ByteArrayInputStream(
          Base64.decodeBase64(base64));
      HistoryEvent event = ReflectionUtils.createClazzInstance(eventClass);
      event.fromProtoStream(in);
      return event;
    }

    public String serialize() throws IOException {
      StringBuilder builder = new StringBuilder();
      builder.append(timing.name() + ",");
      builder.append(encodeHistoryEvent(event));
      return builder.toString();
    }

    public ShutdownCondition deserialize(String str) throws IOException {
      String[] tokens = str.split(",");
      timing = TIMING.valueOf(tokens[0]);
      this.event = decodeHistoryEvent(tokens[1], tokens[2]);
      return this;
    }

    public HistoryEvent getEvent() {
      return event;
    }

    public TIMING getTiming() {
      return timing;
    }
  }

  public static class MyAMShutdownController extends AMShutdownController {

    public static final String SHUTDOWN_CONDITION = "tez.test.recovery.shutdown_condition";

    private ShutdownCondition shutdownCondition;

    public MyAMShutdownController(AppContext appContext,
        RecoveryService recoveryService) {
      super(appContext, recoveryService);
      this.shutdownCondition = new ShutdownCondition();
      try {
        this.shutdownCondition.deserialize(appContext.getAMConf().get(
            SHUTDOWN_CONDITION));
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Fail to load shutdown condition");
      }
    }

    @Override
    protected boolean shouldShutdownPreEvent(DAGHistoryEvent event,
        List<DAGHistoryEvent> historyEvents) {
      return false;
    }

    @Override
    protected boolean shouldShutdownPostEvent(DAGHistoryEvent event,
        List<DAGHistoryEvent> historyEvents) {
      if (appContext.getApplicationAttemptId().getAttemptId() == 1) {
        switch (event.getHistoryEvent().getEventType()) {
        case DAG_SUBMITTED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.DAG_SUBMITTED) {
            shutdown();
          }
          break;
        case DAG_INITIALIZED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.DAG_INITIALIZED) {
            shutdown();
          }
          break;

        case DAG_STARTED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.DAG_STARTED) {
            shutdown();
          }
          break;

        case DAG_FINISHED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.DAG_FINISHED) {
            shutdown();
          }
          break;

        case VERTEX_INITIALIZED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.VERTEX_INITIALIZED) {
            VertexInitializedEvent incomingEvent = (VertexInitializedEvent) event
                .getHistoryEvent();
            VertexInitializedEvent conditionEvent = (VertexInitializedEvent) shutdownCondition
                .getEvent();
            if (incomingEvent.getVertexID().getId() == conditionEvent
                .getVertexID().getId()) {
              shutdown();
            }
          }
          break;
        case VERTEX_STARTED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.VERTEX_STARTED) {
            VertexStartedEvent incomingEvent = (VertexStartedEvent) event
                .getHistoryEvent();
            VertexStartedEvent conditionEvent = (VertexStartedEvent) shutdownCondition
                .getEvent();
            if (incomingEvent.getVertexID().getId() == conditionEvent
                .getVertexID().getId()) {
              shutdown();
            }
          }
          break;
        case VERTEX_FINISHED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.VERTEX_FINISHED) {
            VertexFinishedEvent incomingEvent = (VertexFinishedEvent) event
                .getHistoryEvent();
            VertexFinishedEvent conditionEvent = (VertexFinishedEvent) shutdownCondition
                .getEvent();
            if (incomingEvent.getVertexID().getId() == conditionEvent
                .getVertexID().getId()) {
              shutdown();
            }
          }
          break;
        case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.VERTEX_DATA_MOVEMENT_EVENTS_GENERATED) {
            VertexRecoverableEventsGeneratedEvent incommingEvent = (VertexRecoverableEventsGeneratedEvent) event
                .getHistoryEvent();
            VertexRecoverableEventsGeneratedEvent conditionEvent = (VertexRecoverableEventsGeneratedEvent) shutdownCondition
                .getEvent();
            if (incommingEvent.getVertexID().getId() == conditionEvent
                .getVertexID().getId()) {
              if (incommingEvent.getTezEvents().get(0).getEventType() == EventType.ROOT_INPUT_DATA_INFORMATION_EVENT
                  && conditionEvent.getTezEvents().get(0).getEventType() == EventType.ROOT_INPUT_DATA_INFORMATION_EVENT) {
                shutdown();
              } else if (incommingEvent.getTezEvents().get(0).getEventType() == EventType.COMPOSITE_DATA_MOVEMENT_EVENT
                  && conditionEvent.getTezEvents().get(0).getEventType() == EventType.COMPOSITE_DATA_MOVEMENT_EVENT
                  && incommingEvent
                      .getTezEvents()
                      .get(0)
                      .getSourceInfo()
                      .getEdgeVertexName()
                      .equals(
                          conditionEvent.getTezEvents().get(0).getSourceInfo()
                              .getEdgeVertexName())) {
                shutdown();
              } else if (incommingEvent.getTezEvents().get(0).getEventType() == EventType.DATA_MOVEMENT_EVENT
                  && conditionEvent.getTezEvents().get(0).getEventType() == EventType.DATA_MOVEMENT_EVENT
                  && incommingEvent
                      .getTezEvents()
                      .get(0)
                      .getSourceInfo()
                      .getEdgeVertexName()
                      .equals(
                          conditionEvent.getTezEvents().get(0).getSourceInfo()
                              .getEdgeVertexName())) {
                shutdown();
              }
            }
          }
          break;
        case TASK_STARTED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.TASK_STARTED) {
            TaskStartedEvent incomingEvent = (TaskStartedEvent) event
                .getHistoryEvent();
            TaskStartedEvent conditionEvent = (TaskStartedEvent) shutdownCondition
                .getEvent();
            if (incomingEvent.getTaskID().getVertexID().getId() == conditionEvent
                .getTaskID().getVertexID().getId()
                && incomingEvent.getTaskID().getId() == conditionEvent
                    .getTaskID().getId()) {
              shutdown();
            }
          }
          break;
        case TASK_FINISHED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.TASK_FINISHED) {
            TaskFinishedEvent incomingEvent = (TaskFinishedEvent) event
                .getHistoryEvent();
            TaskFinishedEvent conditionEvent = (TaskFinishedEvent) shutdownCondition
                .getEvent();
            if (incomingEvent.getTaskID().getVertexID().getId() == conditionEvent
                .getTaskID().getVertexID().getId()
                && incomingEvent.getTaskID().getId() == conditionEvent
                    .getTaskID().getId()) {
              shutdown();
            }
          }
          break;

        case TASK_ATTEMPT_STARTED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.TASK_ATTEMPT_STARTED) {
            TaskAttemptStartedEvent incomingEvent = (TaskAttemptStartedEvent) event
                .getHistoryEvent();
            TaskAttemptStartedEvent conditionEvent = (TaskAttemptStartedEvent) shutdownCondition
                .getEvent();
            if (incomingEvent.getTaskAttemptID().getTaskID().getVertexID()
                .getId() == conditionEvent.getTaskAttemptID().getTaskID()
                .getVertexID().getId()
                && incomingEvent.getTaskAttemptID().getTaskID().getId() == conditionEvent
                    .getTaskAttemptID().getTaskID().getId()
                && incomingEvent.getTaskAttemptID().getId() == conditionEvent
                    .getTaskAttemptID().getId()) {
              shutdown();
            }
          }
          break;

        case TASK_ATTEMPT_FINISHED:
          if (shutdownCondition.getEvent().getEventType() == HistoryEventType.TASK_ATTEMPT_FINISHED) {
            TaskAttemptFinishedEvent incomingEvent = (TaskAttemptFinishedEvent) event
                .getHistoryEvent();
            TaskAttemptFinishedEvent conditionEvent = (TaskAttemptFinishedEvent) shutdownCondition
                .getEvent();
            if (incomingEvent.getTaskAttemptID().getTaskID().getVertexID()
                .getId() == conditionEvent.getTaskAttemptID().getTaskID()
                .getVertexID().getId()
                && incomingEvent.getTaskAttemptID().getTaskID().getId() == conditionEvent
                    .getTaskAttemptID().getTaskID().getId()
                && incomingEvent.getTaskAttemptID().getId() == conditionEvent
                    .getTaskAttemptID().getId()) {
              shutdown();
            }
          }
          break;

        default:
          LOG.info("do nothing with event:"
              + event.getHistoryEvent().getEventType());
        }

      }
      return false;
    }

    private void shutdown() {
      Thread shutdownThread = new Thread("MyAMShutdownController thread") {
        @Override
        public void run() {
          System.exit(1);
        }
      };
      recoveryService.setStopped(true);
      shutdownThread.start();
    }
  }

  @Test(timeout = 600000)
  public void testRecovery() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
        1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexId0 = TezVertexID.getInstance(dagId, 0);
    TezVertexID vertexId1 = TezVertexID.getInstance(dagId, 1);
    TezVertexID vertexId2 = TezVertexID.getInstance(dagId, 2);
    ContainerId containerId = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(appId, 1), 1);
    NodeId nodeId = NodeId.newInstance("localhost", 10);
    EventMetaData eventMetaDataV0 = new EventMetaData(
        EventProducerConsumerType.INPUT, "v0", "Tokenizer",
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId0, 0), 0));
    EventMetaData eventMetaDataV1 = new EventMetaData(
        EventProducerConsumerType.INPUT, "v1", "Summation",
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId1, 0), 0));
    EventMetaData eventMetaDataV2 = new EventMetaData(
        EventProducerConsumerType.INPUT, "v2", "Sorter",
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId2, 0), 0));
    List<ShutdownCondition> shutdownConditions = Lists.newArrayList(
        new ShutdownCondition(TIMING.POST, new DAGInitializedEvent(dagId, 0L,
            "username", "dagName", null)),
        new ShutdownCondition(TIMING.POST, new DAGStartedEvent(dagId, 0L,
            "username", "dagName")),
        new ShutdownCondition(TIMING.POST, new DAGFinishedEvent(dagId, 0L, 0L,
            DAGState.SUCCEEDED, "", new TezCounters(), "username", "dagName",
            new HashMap<String, Integer>(), ApplicationAttemptId.newInstance(
                appId, 1))),
        new ShutdownCondition(TIMING.POST, new VertexInitializedEvent(
            vertexId0, "Tokenizer", 0L, 0L, 0, "", null)),
        new ShutdownCondition(TIMING.POST, new VertexInitializedEvent(
            vertexId1, "Summation", 0L, 0L, 0, "", null)),
        new ShutdownCondition(TIMING.POST, new VertexInitializedEvent(
            vertexId2, "Sorter", 0L, 0L, 0, "", null)),

        new ShutdownCondition(TIMING.POST, new VertexStartedEvent(TezVertexID
            .getInstance(dagId, 0), 0L, 0L)),
        new ShutdownCondition(TIMING.POST, new VertexStartedEvent(TezVertexID
            .getInstance(dagId, 1), 0L, 0L)),
        new ShutdownCondition(TIMING.POST, new VertexStartedEvent(TezVertexID
            .getInstance(dagId, 2), 0L, 0L)),

        new ShutdownCondition(TIMING.POST, new VertexFinishedEvent(TezVertexID
            .getInstance(dagId, 0), "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
            VertexState.SUCCEEDED, "", new TezCounters(), new VertexStats(),
            new HashMap<String, Integer>())),
        new ShutdownCondition(TIMING.POST, new VertexFinishedEvent(TezVertexID
            .getInstance(dagId, 1), "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
            VertexState.SUCCEEDED, "", new TezCounters(), new VertexStats(),
            new HashMap<String, Integer>())),
        new ShutdownCondition(TIMING.POST, new VertexFinishedEvent(TezVertexID
            .getInstance(dagId, 2), "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
            VertexState.SUCCEEDED, "", new TezCounters(), new VertexStats(),
            new HashMap<String, Integer>())),

        new ShutdownCondition(TIMING.POST,
            new VertexRecoverableEventsGeneratedEvent(vertexId0, Lists
                .newArrayList(new TezEvent(InputDataInformationEvent
                    .createWithSerializedPayload(0,
                        ByteBuffer.wrap(new byte[0])), eventMetaDataV0)))),
        new ShutdownCondition(TIMING.POST,
            new VertexRecoverableEventsGeneratedEvent(vertexId0, Lists
                .newArrayList(new TezEvent(CompositeDataMovementEvent.create(0,
                    1, ByteBuffer.wrap(new byte[0])), eventMetaDataV1)))),
        new ShutdownCondition(TIMING.POST,
            new VertexRecoverableEventsGeneratedEvent(vertexId0, Lists
                .newArrayList(new TezEvent(CompositeDataMovementEvent.create(0,
                    1, ByteBuffer.wrap(new byte[0])), eventMetaDataV2)))),

        new ShutdownCondition(TIMING.POST, new TaskStartedEvent(TezTaskID
            .getInstance(vertexId0, 0), "vertexName", 0L, 0L)),
        new ShutdownCondition(TIMING.POST, new TaskStartedEvent(TezTaskID
            .getInstance(vertexId1, 0), "vertexName", 0L, 0L)),
        new ShutdownCondition(TIMING.POST, new TaskStartedEvent(TezTaskID
            .getInstance(vertexId2, 0), "vertexName", 0L, 0L)),
        new ShutdownCondition(TIMING.POST, new TaskFinishedEvent(TezTaskID
            .getInstance(vertexId0, 0), "vertexName", 0L, 0L, null,
            TaskState.SUCCEEDED, "", new TezCounters(), 0)),
        new ShutdownCondition(TIMING.POST, new TaskFinishedEvent(TezTaskID
            .getInstance(vertexId1, 0), "vertexName", 0L, 0L, null,
            TaskState.SUCCEEDED, "", new TezCounters(), 0)),
        new ShutdownCondition(TIMING.POST, new TaskFinishedEvent(TezTaskID
            .getInstance(vertexId2, 0), "vertexName", 0L, 0L, null,
            TaskState.SUCCEEDED, "", new TezCounters(), 0)),

        new ShutdownCondition(TIMING.POST,
            new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId0, 0), 0), "vertexName", 0L,
                containerId, nodeId, "", "", "")),
        new ShutdownCondition(TIMING.POST,
            new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId1, 0), 0), "vertexName", 0L,
                containerId, nodeId, "", "", "")),
        new ShutdownCondition(TIMING.POST,
            new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId2, 0), 0), "vertexName", 0L,
                containerId, nodeId, "", "", "")),

        new ShutdownCondition(TIMING.POST,
            new TaskAttemptFinishedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId0, 0), 0), "vertexName", 0L, 0L,
                TaskAttemptState.SUCCEEDED, null, "", new TezCounters())),
        new ShutdownCondition(TIMING.POST,
            new TaskAttemptFinishedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId1, 0), 0), "vertexName", 0L, 0L,
                TaskAttemptState.SUCCEEDED, null, "", new TezCounters())),
        new ShutdownCondition(TIMING.POST,
            new TaskAttemptFinishedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId2, 0), 0), "vertexName", 0L, 0L,
                TaskAttemptState.SUCCEEDED, null, "", new TezCounters())));

    for (ShutdownCondition shutdownCondition : shutdownConditions) {
      testOrderedWordCount(shutdownCondition);
    }
  }

  private void testOrderedWordCount(ShutdownCondition shutdownCondition)
      throws Exception {
    String inputDirStr = "/tmp/owc-input/";
    Path inputDir = new Path(inputDirStr);
    Path stagingDirPath = new Path("/tmp/owc-staging-dir");
    remoteFs.mkdirs(inputDir);
    remoteFs.mkdirs(stagingDirPath);
    TestTezJobs.generateOrderedWordCountInput(inputDir, remoteFs);

    String outputDirStr = "/tmp/owc-output/";
    Path outputDir = new Path(outputDirStr);

    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_RECOVERY_SERVICE_CLASS,
        MockRecoveryService.class.getName());
    tezConf.set(MockRecoveryService.AM_SHUTDOWN_CONTROLLER_CLASS,
        MyAMShutdownController.class.getName());
    tezConf.set(MyAMShutdownController.SHUTDOWN_CONDITION,
        shutdownCondition.serialize());
    tezConf.setBoolean(
        RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, false);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;

    try {
      OrderedWordCount job = new OrderedWordCount();
      Assert.assertTrue("OrderedWordCount failed", job.run(tezConf,
          new String[] { inputDirStr, outputDirStr, "2" }, null) == 0);
      TestTezJobs.verifyOutput(outputDir, remoteFs);

    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (tezSession != null) {
        tezSession.stop();
      }
    }

  }

}
