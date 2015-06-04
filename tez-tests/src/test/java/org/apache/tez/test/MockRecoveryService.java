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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;

import com.google.common.base.Preconditions;

public class MockRecoveryService extends RecoveryService {

  public static final String AM_SHUTDOWN_CONTROLLER_CLASS = "tez.test.am.shutdown.controller";
  
  private AMShutdownController amShutdownController;
  private AppContext appContext;

  public MockRecoveryService(AppContext appContext) {
    super(appContext);
    this.appContext = appContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    String clazz = conf.get(AM_SHUTDOWN_CONTROLLER_CLASS);
    Preconditions.checkArgument(clazz != null, "AMShutdownController is not specified");
    amShutdownController = ReflectionUtils.createClazzInstance(clazz, new Class[]{AppContext.class, RecoveryService.class},
        new Object[] {appContext, this});
  }

  @Override
  protected void handleRecoveryEvent(DAGHistoryEvent event) throws IOException {
    amShutdownController.preAddHistoryEvent(event);
    super.handleRecoveryEvent(event);
    amShutdownController.postAddHistoryEvent(event);
  }

}
