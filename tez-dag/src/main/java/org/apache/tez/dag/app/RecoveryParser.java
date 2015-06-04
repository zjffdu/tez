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

package org.apache.tez.dag.app;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexRecoverableEventsGeneratedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexParallelismUpdatedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.SummaryEventProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class RecoveryParser {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryParser.class);

  private final DAGAppMaster dagAppMaster;
  private final FileSystem recoveryFS;
  private final Path recoveryDataDir;
  private final Path currentAttemptRecoveryDataDir;
  private final int recoveryBufferSize;
  private final int currentAttemptId;

  public RecoveryParser(DAGAppMaster dagAppMaster,
      FileSystem recoveryFS,
      Path recoveryDataDir,
      int currentAttemptId) throws IOException {
    this.dagAppMaster = dagAppMaster;
    this.recoveryFS = recoveryFS;
    this.recoveryDataDir = recoveryDataDir;
    this.currentAttemptId = currentAttemptId;
    this.currentAttemptRecoveryDataDir = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir,
        currentAttemptId);
    recoveryBufferSize = dagAppMaster.getConfig().getInt(
        TezConfiguration.DAG_RECOVERY_FILE_IO_BUFFER_SIZE,
        TezConfiguration.DAG_RECOVERY_FILE_IO_BUFFER_SIZE_DEFAULT);
    this.recoveryFS.mkdirs(currentAttemptRecoveryDataDir);
  }

  public static class RecoveredDAGData {
    public TezDAGID recoveredDagID = null;
    public DAGImpl recoveredDAG = null;
    public DAGState dagState = null;
    public boolean isCompleted = false;
    public boolean nonRecoverable = false;
    public String reason = null;
    public Map<String, LocalResource> cumulativeAdditionalResources = null;
    
    public List<HistoryEvent> dagRecoveryEvents = new ArrayList<HistoryEvent>();
    public Map<TezVertexID, RecoveredVertexData> vertexRecoveryData =
        new HashMap<TezVertexID, RecoveryParser.RecoveredVertexData>();
    public boolean recoveredInitedEventSeen = false;
    public boolean recoveredStartedEventSeen = false;
    public boolean recoveredFinishedEventSeen = false;
  }

  private static void parseSummaryFile(FSDataInputStream inputStream)
      throws IOException {
    while (true) {
      RecoveryProtos.SummaryEventProto proto =
          RecoveryProtos.SummaryEventProto.parseDelimitedFrom(inputStream);
      if (proto == null) {
        LOG.info("Reached end of summary stream");
        break;
      }
      LOG.info("[SUMMARY]"
          + " dagId=" + proto.getDagId()
          + ", timestamp=" + proto.getTimestamp()
          + ", event=" + HistoryEventType.values()[proto.getEventType()]);
    }
  }

  private static HistoryEvent getNextEvent(FSDataInputStream inputStream)
      throws IOException {
    int eventTypeOrdinal = -1;
    try {
      eventTypeOrdinal = inputStream.readInt();
    } catch (EOFException eof) {
      return null;
    }
    if (eventTypeOrdinal < 0 || eventTypeOrdinal >=
        HistoryEventType.values().length) {
      // Corrupt data
      // reached end
      throw new IOException("Corrupt data found when trying to read next event type"
          + ", eventTypeOrdinal=" + eventTypeOrdinal);
    }
    HistoryEventType eventType = HistoryEventType.values()[eventTypeOrdinal];
    HistoryEvent event;
    switch (eventType) {
      case AM_LAUNCHED:
        event = new AMLaunchedEvent();
        break;
      case AM_STARTED:
        event = new AMStartedEvent();
        break;
      case DAG_SUBMITTED:
        event = new DAGSubmittedEvent();
        break;
      case DAG_INITIALIZED:
        event = new DAGInitializedEvent();
        break;
      case DAG_STARTED:
        event = new DAGStartedEvent();
        break;
      case DAG_COMMIT_STARTED:
        event = new DAGCommitStartedEvent();
        break;
      case DAG_FINISHED:
        event = new DAGFinishedEvent();
        break;
      case CONTAINER_LAUNCHED:
        event = new ContainerLaunchedEvent();
        break;
      case CONTAINER_STOPPED:
        event = new ContainerStoppedEvent();
        break;
      case VERTEX_INITIALIZED:
        event = new VertexInitializedEvent();
        break;
      case VERTEX_STARTED:
        event = new VertexStartedEvent();
        break;
      case VERTEX_PARALLELISM_UPDATED:
        event = new VertexParallelismUpdatedEvent();
        break;
      case VERTEX_COMMIT_STARTED:
        event = new VertexCommitStartedEvent();
        break;
      case VERTEX_GROUP_COMMIT_STARTED:
        event = new VertexGroupCommitStartedEvent();
        break;
      case VERTEX_GROUP_COMMIT_FINISHED:
        event = new VertexGroupCommitFinishedEvent();
        break;
      case VERTEX_FINISHED:
        event = new VertexFinishedEvent();
        break;
      case TASK_STARTED:
        event = new TaskStartedEvent();
        break;
      case TASK_FINISHED:
        event = new TaskFinishedEvent();
        break;
      case TASK_ATTEMPT_STARTED:
        event = new TaskAttemptStartedEvent();
        break;
      case TASK_ATTEMPT_FINISHED:
        event = new TaskAttemptFinishedEvent();
        break;
      case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
        event = new VertexRecoverableEventsGeneratedEvent();
        break;
      default:
        throw new IOException("Invalid data found, unknown event type "
            + eventType);

    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsing event from input stream"
          + ", eventType=" + eventType);
    }
    try {
      event.fromProtoStream(inputStream);
    } catch (EOFException eof) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsed event from input stream"
          + ", eventType=" + eventType
          + ", event=" + event.toString());
    }
    return event;
  }





  public static List<HistoryEvent> parseDAGRecoveryFile(FSDataInputStream inputStream)
      throws IOException {
    List<HistoryEvent> historyEvents = new ArrayList<HistoryEvent>();
    while (true) {
      HistoryEvent historyEvent = getNextEvent(inputStream);
      if (historyEvent == null) {
        LOG.info("Reached end of stream");
        break;
      }
      historyEvents.add(historyEvent);
    }
    return historyEvents;
  }

  public static void main(String argv[]) throws IOException {
    // TODO clean up with better usage and error handling
    Configuration conf = new Configuration();
    String summaryPath = argv[0];
    List<String> dagPaths = new ArrayList<String>();
    if (argv.length > 1) {
      for (int i = 1; i < argv.length; ++i) {
        dagPaths.add(argv[i]);
      }
    }
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Parsing Summary file " + summaryPath);
    parseSummaryFile(fs.open(new Path(summaryPath)));
    for (String dagPath : dagPaths) {
      LOG.info("Parsing DAG recovery file " + dagPath);
      List<HistoryEvent> historyEvents = parseDAGRecoveryFile(fs.open(new Path(dagPath)));
      for (HistoryEvent historyEvent : historyEvents) {
        LOG.info("Parsed event from recovery stream"
            + ", eventType=" + historyEvent.getEventType()
            + ", event=" + historyEvent);
      }
    }
  }

  private Path getSummaryPath(Path attemptRrecoveryDataDir) {
    return TezCommonUtils.getSummaryRecoveryPath(attemptRrecoveryDataDir);
  }

  private FSDataInputStream getSummaryStream(Path summaryPath)
      throws IOException {
    if (!recoveryFS.exists(summaryPath)) {
      return null;
    }
    return recoveryFS.open(summaryPath, recoveryBufferSize);
  }

  private Path getDAGRecoveryFilePath(Path recoveryDataDir,
      TezDAGID dagID) {
    return new Path(recoveryDataDir,
        dagID.toString() + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
  }

  @VisibleForTesting
  DAGSummaryData getLastCompletedOrInProgressDAG(
      Map<TezDAGID, DAGSummaryData> dagSummaryDataMap) {
    DAGSummaryData inProgressDAG = null;
    DAGSummaryData lastCompletedDAG = null;
    for (Map.Entry<TezDAGID, DAGSummaryData> entry : dagSummaryDataMap.entrySet()) {
      if (!entry.getValue().completed) {
        if (inProgressDAG != null) {
          throw new RuntimeException("Multiple in progress DAGs seen"
              + ", dagId=" + inProgressDAG.dagId
              + ", dagId=" + entry.getKey());
        }
        inProgressDAG = entry.getValue();
      } else {
        if (lastCompletedDAG == null ||
            lastCompletedDAG.dagId.getId() < entry.getValue().dagId.getId()) {
          lastCompletedDAG = entry.getValue();
        }
      }
    }
    if (inProgressDAG == null) {
      return lastCompletedDAG;
    }
    return inProgressDAG;
  }

  @VisibleForTesting
  static class DAGSummaryData {

    final TezDAGID dagId;
    String dagName;
    boolean completed = false;
    boolean dagCommitCompleted = true;
    DAGState dagState;
    Map<TezVertexID, Boolean> vertexCommitStatus =
        new HashMap<TezVertexID, Boolean>();
    Map<String, Boolean> vertexGroupCommitStatus =
        new HashMap<String, Boolean>();
    List<HistoryEvent> bufferedSummaryEvents =
        new ArrayList<HistoryEvent>();

    DAGSummaryData(TezDAGID dagId) {
      this.dagId = dagId;
    }

    void handleSummaryEvent(SummaryEventProto proto) throws IOException {
      HistoryEventType eventType =
          HistoryEventType.values()[proto.getEventType()];
      switch (eventType) {
        case DAG_SUBMITTED:
          completed = false;
          DAGSubmittedEvent dagSubmittedEvent = new DAGSubmittedEvent();
          dagSubmittedEvent.fromSummaryProtoStream(proto);
          dagName = dagSubmittedEvent.getDAGName();
          break;
        case DAG_FINISHED:
          completed = true;
          dagCommitCompleted = true;
          DAGFinishedEvent dagFinishedEvent = new DAGFinishedEvent();
          dagFinishedEvent.fromSummaryProtoStream(proto);
          dagState = dagFinishedEvent.getState();
          break;
        case DAG_COMMIT_STARTED:
          dagCommitCompleted = false;
          break;
        case VERTEX_COMMIT_STARTED:
          VertexCommitStartedEvent vertexCommitStartedEvent =
              new VertexCommitStartedEvent();
          vertexCommitStartedEvent.fromSummaryProtoStream(proto);
          vertexCommitStatus.put(
              vertexCommitStartedEvent.getVertexID(), false);
          break;
        case VERTEX_FINISHED:
          VertexFinishedEvent vertexFinishedEvent =
              new VertexFinishedEvent();
          vertexFinishedEvent.fromSummaryProtoStream(proto);
          if (vertexCommitStatus.containsKey(vertexFinishedEvent.getVertexID())) {
            vertexCommitStatus.put(
                vertexFinishedEvent.getVertexID(), true);
            bufferedSummaryEvents.add(vertexFinishedEvent);
          }
          break;
        case VERTEX_GROUP_COMMIT_STARTED:
          VertexGroupCommitStartedEvent vertexGroupCommitStartedEvent =
              new VertexGroupCommitStartedEvent();
          vertexGroupCommitStartedEvent.fromSummaryProtoStream(proto);
          bufferedSummaryEvents.add(vertexGroupCommitStartedEvent);
          vertexGroupCommitStatus.put(
              vertexGroupCommitStartedEvent.getVertexGroupName(), false);
          break;
        case VERTEX_GROUP_COMMIT_FINISHED:
          VertexGroupCommitFinishedEvent vertexGroupCommitFinishedEvent =
              new VertexGroupCommitFinishedEvent();
          vertexGroupCommitFinishedEvent.fromSummaryProtoStream(proto);
          bufferedSummaryEvents.add(vertexGroupCommitFinishedEvent);
          vertexGroupCommitStatus.put(
              vertexGroupCommitFinishedEvent.getVertexGroupName(), true);
          break;
        default:
          String message = "Found invalid summary event that was not handled"
              + ", eventType=" + eventType.name();
          throw new IOException(message);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("dagId=").append(dagId);
      sb.append(", dagCompleted=").append(completed);
      if (!vertexCommitStatus.isEmpty()) {
        sb.append(", vertexCommitStatuses=[");
        for (Entry<TezVertexID, Boolean> entry : vertexCommitStatus.entrySet()) {
          sb.append("{ vertexId=").append(entry.getKey())
              .append(", committed=").append(entry.getValue()).append("}, ");
        }
        sb.append("]");
      }
      if (!vertexGroupCommitStatus.isEmpty()) {
        sb.append(", vertexGroupCommitStatuses=[");
        for (Entry<String, Boolean> entry : vertexGroupCommitStatus.entrySet()) {
          sb.append("{ vertexGroup=").append(entry.getKey())
              .append(", committed=").append(entry.getValue()).append("}, ");
        }
        sb.append("]");
      }
      return sb.toString();
    }
  }

  private String isDAGRecoverable(DAGSummaryData data) {
    if (!data.dagCommitCompleted) {
      return "DAG Commit was in progress, not recoverable"
          + ", dagId=" + data.dagId;
    }
    if (!data.vertexCommitStatus.isEmpty()) {
      for (Entry<TezVertexID, Boolean> entry : data.vertexCommitStatus.entrySet()) {
        if (!(entry.getValue().booleanValue())) {
          return "Vertex Commit was in progress, not recoverable"
              + ", dagId=" + data.dagId
              + ", vertexId=" + entry.getKey();
        }
      }
    }
    if (!data.vertexGroupCommitStatus.isEmpty()) {
      for (Entry<String, Boolean> entry : data.vertexGroupCommitStatus.entrySet()) {
        if (!(entry.getValue().booleanValue())) {
          return "Vertex Group Commit was in progress, not recoverable"
              + ", dagId=" + data.dagId
              + ", vertexGroup=" + entry.getKey();
        }
      }
    }
    return null;
  }

  private List<Path> getSummaryFiles() throws IOException {
    List<Path> summaryFiles = new ArrayList<Path>();
    for (int i = 1; i < currentAttemptId; ++i) {
      Path attemptPath = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir, i);
      Path fatalErrorOccurred = new Path(attemptPath,
          RecoveryService.RECOVERY_FATAL_OCCURRED_DIR);
      if (recoveryFS.exists(fatalErrorOccurred)) {
        throw new IOException("Found that a fatal error occurred in"
            + " recovery during previous attempt, foundFile="
            + fatalErrorOccurred.toString());
      }
      Path summaryFile = getSummaryPath(attemptPath);
      if (recoveryFS.exists(summaryFile)) {
        summaryFiles.add(summaryFile);
      }
    }
    return summaryFiles;
  }

  private List<Path> getDAGRecoveryFiles(TezDAGID dagId) throws IOException {
    List<Path> recoveryFiles = new ArrayList<Path>();
    for (int i = 1; i < currentAttemptId; ++i) {
      Path attemptPath = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir, i);
      Path recoveryFile = getDAGRecoveryFilePath(attemptPath, dagId);
      if (recoveryFS.exists(recoveryFile)) {
        recoveryFiles.add(recoveryFile);
      }
    }
    return recoveryFiles;
  }

  public RecoveredDAGData parseRecoveryData() throws IOException {
    int dagCounter = 0;
    Map<TezDAGID, DAGSummaryData> dagSummaryDataMap =
        new HashMap<TezDAGID, DAGSummaryData>();
    List<Path> summaryFiles = getSummaryFiles();
    for (Path summaryFile : summaryFiles) {
      FileStatus summaryFileStatus = recoveryFS.getFileStatus(summaryFile);
      LOG.info("Parsing summary file"
          + ", path=" + summaryFile.toString()
          + ", len=" + summaryFileStatus.getLen()
          + ", lastModTime=" + summaryFileStatus.getModificationTime());
      FSDataInputStream summaryStream = getSummaryStream(
          summaryFile);
      while (true) {
        RecoveryProtos.SummaryEventProto proto;
        try {
          proto = RecoveryProtos.SummaryEventProto.parseDelimitedFrom(summaryStream);
          if (proto == null) {
            LOG.info("Reached end of summary stream");
            break;
          }
        } catch (EOFException eof) {
          LOG.info("Reached end of summary stream");
          break;
        }
        HistoryEventType eventType =
            HistoryEventType.values()[proto.getEventType()];
        if (LOG.isDebugEnabled()) {
          LOG.debug("[RECOVERY SUMMARY]"
              + " dagId=" + proto.getDagId()
              + ", timestamp=" + proto.getTimestamp()
              + ", event=" + eventType);
        }
        TezDAGID dagId = TezDAGID.fromString(proto.getDagId());
        if (dagId == null) {
          throw new IOException("null dagId, summary records may be corrupted");
        }
        if (dagCounter < dagId.getId()) {
          dagCounter = dagId.getId();
        }
        if (!dagSummaryDataMap.containsKey(dagId)) {
          dagSummaryDataMap.put(dagId, new DAGSummaryData(dagId));
        }
        try {
          dagSummaryDataMap.get(dagId).handleSummaryEvent(proto);
        } catch (Exception e) {
          // any exception when parsing protobuf
          throw new IOException("Error when parsing summary event proto", e);
        }
      }
      summaryStream.close();
    }

    // Set counter for next set of DAGs & update dagNames Set in DAGAppMaster
    dagAppMaster.setDAGCounter(dagCounter);
    for (DAGSummaryData dagSummaryData: dagSummaryDataMap.values()){
      dagAppMaster.dagNames.add(dagSummaryData.dagName);
      dagAppMaster.dagIDs.add(dagSummaryData.dagId.toString());
    }

    DAGSummaryData lastInProgressDAGData =
        getLastCompletedOrInProgressDAG(dagSummaryDataMap);
    if (lastInProgressDAGData == null) {
      LOG.info("Nothing to recover as no uncompleted/completed DAGs found");
      return null;
    }
    TezDAGID lastInProgressDAG = lastInProgressDAGData.dagId;
    if (lastInProgressDAG == null) {
      LOG.info("Nothing to recover as no uncompleted/completed DAGs found");
      return null;
    }

    LOG.info("Checking if DAG is in recoverable state"
        + ", dagId=" + lastInProgressDAGData.dagId);

    final RecoveredDAGData recoveredDAGData = new RecoveredDAGData();
    if (lastInProgressDAGData.completed) {
      recoveredDAGData.isCompleted = true;
      recoveredDAGData.dagState = lastInProgressDAGData.dagState;
    }

    String nonRecoverableReason = isDAGRecoverable(lastInProgressDAGData);
    if (nonRecoverableReason != null) {
      LOG.warn("Found last inProgress DAG but not recoverable: "
          + lastInProgressDAGData);
      recoveredDAGData.nonRecoverable = true;
      recoveredDAGData.reason = nonRecoverableReason;
    }

    List<Path> dagRecoveryFiles = getDAGRecoveryFiles(lastInProgressDAG);
    boolean skipAllOtherEvents = false;
    Path lastRecoveryFile = null;
    for (Path dagRecoveryFile : dagRecoveryFiles) {
      if (skipAllOtherEvents) {
        LOG.warn("Other recovery files will be skipped due to error in the previous recovery file"
            + lastRecoveryFile);
        break;
      }
      lastRecoveryFile = dagRecoveryFile;
      LOG.info("Trying to recover dag from recovery file"
          + ", dagId=" + lastInProgressDAG.toString()
          + ", dagRecoveryFile=" + dagRecoveryFile);
      FSDataInputStream dagRecoveryStream = recoveryFS.open(dagRecoveryFile, recoveryBufferSize);
      while (true) {
        HistoryEvent event;
        try {
          event = getNextEvent(dagRecoveryStream);
          if (event == null) {
            LOG.info("Reached end of dag recovery stream");
            break;
          }
        } catch (EOFException eof) {
          LOG.info("Reached end of dag recovery stream");
          break;
        } catch (IOException ioe) {
          LOG.warn("Corrupt data found when trying to read next event", ioe);
          break;
        }
        if (skipAllOtherEvents) {
          // hit an error - skip reading other events
          break;
        }

        HistoryEventType eventType = event.getEventType();
        LOG.info("Recovering from event"
            + ", eventType=" + eventType
            + ", event=" + event.toString());
        switch (eventType) {
          case DAG_SUBMITTED:
            DAGSubmittedEvent submittedEvent = (DAGSubmittedEvent) event;
            recoveredDAGData.recoveredDAG = dagAppMaster.createDAG(submittedEvent.getDAGPlan(),
                lastInProgressDAG);
            recoveredDAGData.cumulativeAdditionalResources = submittedEvent
              .getCumulativeAdditionalLocalResources();
            recoveredDAGData.recoveredDagID = recoveredDAGData.recoveredDAG.getID();
            dagAppMaster.setCurrentDAG(recoveredDAGData.recoveredDAG);
            if (recoveredDAGData.nonRecoverable) {
              skipAllOtherEvents = true;
            }
            break;
          case DAG_INITIALIZED:
            recoveredDAGData.recoveredInitedEventSeen = true;
            recoveredDAGData.dagRecoveryEvents.add(event);
            break;
          case DAG_STARTED:
            recoveredDAGData.recoveredStartedEventSeen = true;
            recoveredDAGData.dagRecoveryEvents.add(event);
            break;

          case DAG_FINISHED:
            recoveredDAGData.recoveredFinishedEventSeen = true;
            recoveredDAGData.dagRecoveryEvents.add(event);
            break; 
            
          case DAG_COMMIT_STARTED:
          case VERTEX_GROUP_COMMIT_STARTED:
          case VERTEX_GROUP_COMMIT_FINISHED: 
          case CONTAINER_LAUNCHED:
          {
            // Nothing to do for now
            break;
          }
          case VERTEX_INITIALIZED:
          {
            VertexInitializedEvent vertexInitEvent = (VertexInitializedEvent)event;
            RecoveredVertexData vertexRecoveryData = recoveredDAGData.vertexRecoveryData.get(vertexInitEvent.getVertexID());
            if (vertexRecoveryData == null) {
              vertexRecoveryData = new RecoveredVertexData();
              recoveredDAGData.vertexRecoveryData.put(vertexInitEvent.getVertexID(), vertexRecoveryData);
            }
            vertexRecoveryData.vertexRecoveryEvents.add(vertexInitEvent);
            vertexRecoveryData.recoveredInitedEventSeen = true;
            break;
          }
          case VERTEX_STARTED:
          {
            VertexStartedEvent vertexStartedEvent = (VertexStartedEvent)event;
            RecoveredVertexData vertexRecoveryData = recoveredDAGData.vertexRecoveryData.get(vertexStartedEvent.getVertexID());
            vertexRecoveryData.vertexRecoveryEvents.add(vertexStartedEvent);
            vertexRecoveryData.recoveredStartedEventSeen = true;
            break;
          }
          case VERTEX_PARALLELISM_UPDATED:
          {
            VertexParallelismUpdatedEvent vertexParallelismUpdatedEvent = (VertexParallelismUpdatedEvent)event;
            RecoveredVertexData vertexRecoveryData = recoveredDAGData.vertexRecoveryData.get(vertexParallelismUpdatedEvent.getVertexID());
            vertexRecoveryData.vertexRecoveryEvents.add(vertexParallelismUpdatedEvent);
            break;
          }
          case VERTEX_COMMIT_STARTED:
          {
            VertexCommitStartedEvent vertexCommitStartedEvent = (VertexCommitStartedEvent)event;
            RecoveredVertexData vertexRecoveryData = recoveredDAGData.vertexRecoveryData.get(vertexCommitStartedEvent.getVertexID());
            vertexRecoveryData.vertexRecoveryEvents.add(vertexCommitStartedEvent);
            break;
          }
          case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
          {
            VertexRecoverableEventsGeneratedEvent vEvent =
                (VertexRecoverableEventsGeneratedEvent) event;
            RecoveredVertexData vertexRecoveryData = recoveredDAGData.vertexRecoveryData.get(vEvent.getVertexID());
            // VertexDataMovementEvent may be logged before VertexInitializedEvent (InputDataInfo for root vertices )
            if (vertexRecoveryData == null) {
              vertexRecoveryData = new RecoveredVertexData();
              recoveredDAGData.vertexRecoveryData.put(vEvent.getVertexID(), vertexRecoveryData);
            }
            vertexRecoveryData.vertexRecoveryEvents.add(vEvent);
            break;
          }
          case VERTEX_FINISHED:
          {
            VertexFinishedEvent vertexFinishedEvent = (VertexFinishedEvent)event;
            RecoveredVertexData vertexRecoveryData = recoveredDAGData.vertexRecoveryData.get(vertexFinishedEvent.getVertexID());
            vertexRecoveryData.vertexRecoveryEvents.add(vertexFinishedEvent);
            vertexRecoveryData.recoveredFinishedEventSeen = true;
            break;
          }
          case TASK_STARTED:
          {
            TaskStartedEvent taskStartedEvent = (TaskStartedEvent) event;
            RecoveredVertexData recoveredVertexData = recoveredDAGData.vertexRecoveryData.get(taskStartedEvent.getTaskID().getVertexID());
            Preconditions.checkArgument(recoveredVertexData != null,
                "Invalid taskStartedEvent, its vertex does not exist:" + taskStartedEvent.getTaskID().getVertexID());
            RecoveredTaskData recoveredTaskData = recoveredVertexData.taskRecoveryData.get(taskStartedEvent.getTaskID());
            if (recoveredTaskData == null) {
              recoveredTaskData = new RecoveredTaskData();
              recoveredVertexData.taskRecoveryData.put(taskStartedEvent.getTaskID(), recoveredTaskData);
            }
            recoveredTaskData.taskRecoveryEvents.add(taskStartedEvent);
            recoveredTaskData.recoveredStartedEventSeen = true;
            break;
          }
          case TASK_FINISHED:
          {
            TaskFinishedEvent taskFinishedEvent = (TaskFinishedEvent) event;
            RecoveredVertexData recoveredVertexData = recoveredDAGData.vertexRecoveryData.get(taskFinishedEvent.getTaskID().getVertexID());
            RecoveredTaskData recoveredTaskData = recoveredVertexData.taskRecoveryData.get(taskFinishedEvent.getTaskID());
            if (recoveredTaskData == null) {
              recoveredTaskData = new RecoveredTaskData();
              recoveredVertexData.taskRecoveryData.put(taskFinishedEvent.getTaskID(), recoveredTaskData);
            }
            recoveredTaskData.taskRecoveryEvents.add(taskFinishedEvent);
            recoveredTaskData.recoveredFinishedEventSeen = true;
            break;
          }
          case TASK_ATTEMPT_STARTED:
          {
            TaskAttemptStartedEvent taStartedEvent = (TaskAttemptStartedEvent)event;
            RecoveredTaskData recoveredTaskData = recoveredDAGData.vertexRecoveryData
                .get(taStartedEvent.getTaskAttemptID().getTaskID().getVertexID())
                .taskRecoveryData.get(taStartedEvent.getTaskAttemptID().getTaskID());
            Preconditions.checkArgument(recoveredTaskData != null,
                "Invalid TaskAttemptStartedEvent, its taskId does not exist, taskId=" + taStartedEvent.getTaskAttemptID().getTaskID());
            RecoveredTaskAttemptData recoveredTaskAttemptData = recoveredTaskData.taskAttemptRecoveryData.get(taStartedEvent.getTaskAttemptID());
            
            if (recoveredTaskAttemptData == null) {
              recoveredTaskAttemptData = new RecoveredTaskAttemptData();
              recoveredTaskData.taskAttemptRecoveryData.put(taStartedEvent.getTaskAttemptID(), recoveredTaskAttemptData);
            }
            recoveredTaskAttemptData.taskAttemptRecoveryEvent.add(taStartedEvent);
            recoveredTaskAttemptData.recoveredStartedEventSeen = true;
            break;
          }
          case TASK_ATTEMPT_FINISHED:
          {
            TaskAttemptFinishedEvent taFinishedEvent = (TaskAttemptFinishedEvent)event;
            RecoveredTaskData recoveredTaskData = recoveredDAGData.vertexRecoveryData
                .get(taFinishedEvent.getTaskAttemptID().getTaskID().getVertexID())
                .taskRecoveryData.get(taFinishedEvent.getTaskAttemptID().getTaskID());
            Preconditions.checkArgument(recoveredTaskData != null,
                "Invalid TaskAttemptFinishedEvent, its taskId does not exist, taskId=" + taFinishedEvent.getTaskAttemptID().getTaskID());
            RecoveredTaskAttemptData recoveredTaskAttemptData = recoveredTaskData.taskAttemptRecoveryData.get(taFinishedEvent.getTaskAttemptID());
            
            if (recoveredTaskAttemptData == null) {
              recoveredTaskAttemptData = new RecoveredTaskAttemptData();
              recoveredTaskData.taskAttemptRecoveryData.put(taFinishedEvent.getTaskAttemptID(), recoveredTaskAttemptData);
            }
            recoveredTaskAttemptData.taskAttemptRecoveryEvent.add(taFinishedEvent);
            recoveredTaskAttemptData.recoveredFinishedEventSeen = true;
            break;
          }
          default:
            throw new RuntimeException("Invalid data found, unknown event type "
                + eventType);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("[DAG RECOVERY]"
              + " dagId=" + lastInProgressDAG
              + ", eventType=" + eventType
              + ", event=" + event.toString());
        }
      }
      dagRecoveryStream.close();
    }

//    if (!recoveredDAGData.isCompleted
//        && !recoveredDAGData.nonRecoverable) {
//      if (lastInProgressDAGData.bufferedSummaryEvents != null
//        && !lastInProgressDAGData.bufferedSummaryEvents.isEmpty()) {
//        for (HistoryEvent bufferedEvent : lastInProgressDAGData.bufferedSummaryEvents) {
//          assert recoveredDAGData.recoveredDAG != null;
//          switch (bufferedEvent.getEventType()) {
//            case VERTEX_GROUP_COMMIT_STARTED:
//              recoveredDAGData.recoveredDAG.restoreFromEvent(bufferedEvent);
//              break;
//            case VERTEX_GROUP_COMMIT_FINISHED:
//              recoveredDAGData.recoveredDAG.restoreFromEvent(bufferedEvent);
//              break;
//            case VERTEX_FINISHED:
//              VertexFinishedEvent vertexFinishedEvent =
//                  (VertexFinishedEvent) bufferedEvent;
//              Vertex vertex = recoveredDAGData.recoveredDAG.getVertex(
//                  vertexFinishedEvent.getVertexID());
//              if (vertex == null) {
//                recoveredDAGData.nonRecoverable = true;
//                recoveredDAGData.reason = "All state could not be recovered"
//                    + ", vertex completed but events not flushed"
//                    + ", vertexId=" + vertexFinishedEvent.getVertexID();
//              } else {
//                vertex.restoreFromEvent(vertexFinishedEvent);
//              }
//              break;
//            default:
//              throw new RuntimeException("Invalid data found in buffered summary events"
//                  + ", unknown event type "
//                  + bufferedEvent.getEventType());
//          }
//        }
//      }
//    }

    return recoveredDAGData;
  }
  
  public static class RecoveredVertexData {
    private List<HistoryEvent> vertexRecoveryEvents = new ArrayList<HistoryEvent>();
    private Map<TezTaskID, RecoveredTaskData> taskRecoveryData =
        new HashMap<TezTaskID, RecoveryParser.RecoveredTaskData>();
    public boolean recoveredInitedEventSeen = false;
    public boolean recoveredStartedEventSeen = false;
    public boolean recoveredFinishedEventSeen = false;

    public List<HistoryEvent> getVertexRecoveryEvents() {
      return vertexRecoveryEvents;
    }
    
    public Map<TezTaskID, RecoveredTaskData> getTaskRecoveryData() {
      return taskRecoveryData;
    }
  }
  
  public static class RecoveredTaskData {
    private List<HistoryEvent> taskRecoveryEvents = new ArrayList<HistoryEvent>();
    private Map<TezTaskAttemptID, RecoveredTaskAttemptData> taskAttemptRecoveryData =
        new HashMap<TezTaskAttemptID, RecoveryParser.RecoveredTaskAttemptData>();
    public boolean recoveredStartedEventSeen = false;
    public boolean recoveredFinishedEventSeen = false;
    
    public List<HistoryEvent> getTaskRecoveryEvents() {
      return taskRecoveryEvents;
    }
    
    public Map<TezTaskAttemptID, RecoveredTaskAttemptData> getTaskAttemptRecoveryData() {
      return taskAttemptRecoveryData;
    }
  }
  
  public static class RecoveredTaskAttemptData {
    private List<HistoryEvent> taskAttemptRecoveryEvent = new ArrayList<HistoryEvent>();
    public boolean recoveredStartedEventSeen = false;
    public boolean recoveredFinishedEventSeen = false;
    public List<HistoryEvent> getTaskAttemptRecoveryEvent() {
      return taskAttemptRecoveryEvent;
    }
  }
}
