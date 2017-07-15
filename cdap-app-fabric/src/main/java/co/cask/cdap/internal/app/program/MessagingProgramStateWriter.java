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

package co.cask.cdap.internal.app.program;

import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * An implementation of ProgramStateWriter that publishes program status events to TMS
 */
public final class MessagingProgramStateWriter implements ProgramStateWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingProgramStateWriter.class);
  private static final Gson GSON = new Gson();
  private final MessagingService messagingService;
  private final TopicId topicId;
  private Map<String, String> userArguments = ImmutableMap.of();
  private Map<String, String> systemArguments = ImmutableMap.of();

  public MessagingProgramStateWriter(CConfiguration cConf, MessagingService messagingService) {
    this.topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC));
    this.messagingService = messagingService;
  }

  @Override
  public void start(ProgramRunId programRunId, String twillRunId, long startTime) {
    publish(
      programRunId, twillRunId,
      ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(startTime))
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramController.State.STARTING.getRunStatus().toString())
        .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(userArguments))
        .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(systemArguments))
    );
  }

  @Override
  public void running(ProgramRunId programRunId, String twillRunId, long startTime) {
    publish(
      programRunId, twillRunId,
      ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(startTime))
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramController.State.ALIVE.getRunStatus().toString())
        .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(userArguments))
        .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(systemArguments))
    );
  }

  @Override
  public void stop(ProgramRunId programRunId, long endTime, ProgramRunStatus runStatus,
                   @Nullable BasicThrowable cause) {
    ImmutableMap.Builder builder = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.END_TIME, String.valueOf(endTime))
      .put(ProgramOptionConstants.PROGRAM_STATUS, runStatus.toString());

    if (cause != null) {
      builder.put("error", GSON.toJson(cause));
    }
//    if (workflowToken != null) {
//      builder.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(workflowToken));
//    }
    publish(programRunId, null, builder);
  }

  @Override
  public void suspend(ProgramRunId programRunId) {
    publish(
      programRunId, null,
      ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.SUSPENDED.toString())
    );
  }

  @Override
  public void resume(ProgramRunId programRunId) {
    publish(
      programRunId, null,
      ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.toString())
    );
  }

  private void publish(ProgramRunId programRunId, String twillRunId, ImmutableMap.Builder<String, String> properties) {
    properties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId));
    if (twillRunId != null) {
      properties.put(ProgramOptionConstants.TWILL_RUN_ID, twillRunId);
    }
    Notification programStatusNotification = new Notification(Notification.Type.PROGRAM_STATUS, properties.build());
    try {
      messagingService.publish(StoreRequestBuilder.of(topicId)
                                                  .addPayloads(GSON.toJson(programStatusNotification))
                                                  .build()
      );
    } catch (Exception e) {
      LOG.warn("Error while publishing notification for program {}: {}", programRunId, e);
    }
  }

  @Override
  public ProgramStateWriter withArguments(Map<String, String> userArguments, Map<String, String> systemArguments) {
    this.userArguments = ImmutableMap.copyOf(userArguments);
    this.systemArguments = ImmutableMap.copyOf(systemArguments);
    return this;
  }
}
