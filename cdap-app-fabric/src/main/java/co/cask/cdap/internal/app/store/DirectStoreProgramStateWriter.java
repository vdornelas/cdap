/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An implementation of the ProgramStateWriter that persists directly to the store
 */
public final class DirectStoreProgramStateWriter implements ProgramStateWriter {
  private final Store store;
  private Map<String, String> userArguments = ImmutableMap.of();
  private Map<String, String> systemArguments = ImmutableMap.of();

  public DirectStoreProgramStateWriter(Store store) {
    this.store = store;
  }

  @Override
  public void start(final ProgramRunId programRunId, final String twillRunId, final long startTime) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setInit(programRunId.getParent(), programRunId.getRun(), TimeUnit.MILLISECONDS.toSeconds(startTime),
                      twillRunId, userArguments, systemArguments);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void running(final ProgramRunId programRunId, final String twillRunId, final long startTime) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setStart(programRunId.getParent(), programRunId.getRun(), TimeUnit.MILLISECONDS.toSeconds(startTime),
                       twillRunId, userArguments, systemArguments);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void stop(final ProgramRunId programRunId, final long endTime, final ProgramRunStatus runStatus,
                   final @Nullable BasicThrowable cause) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setStop(programRunId.getParent(), programRunId.getRun(), TimeUnit.MILLISECONDS.toSeconds(endTime),
                      runStatus, cause);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void suspend(final ProgramRunId programRunId) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setSuspend(programRunId.getParent(), programRunId.getRun());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void resume(final ProgramRunId programRunId) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setResume(programRunId.getParent(), programRunId.getRun());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  public ProgramStateWriter withArguments(Map<String, String> userArguments, Map<String, String> systemArguments) {
    this.userArguments = ImmutableMap.copyOf(userArguments);
    this.systemArguments = ImmutableMap.copyOf(systemArguments);
    return this;
  }

  @Override
  public ProgramStateWriter withWorkflowToken(WorkflowToken workflowToken) {
    throw new UnsupportedOperationException("Cannot add workflow token to DirectStoreProgramStateWriter");
  }
}
