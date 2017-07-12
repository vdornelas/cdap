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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Service that receives program statuses and persists to the store
 */
public class ProgramStatusPersistService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStatusPersistService.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();
  private final MessagingService messagingService;
  private final CConfiguration cConf;

  @Inject
  ProgramStatusPersistService(MessagingService messagingService, Store store, CConfiguration cConf,
                              DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    super(messagingService, store, cConf, datasetFramework, txClient);
    this.messagingService = messagingService;
    this.cConf = cConf;
  }

  @Override
  protected void startUp() {
    LOG.info("Starting ProgramStatusPersistService");

    taskExecutorService =
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("program-status-subscriber-task-%d")
                                                              .build());
    taskExecutorService.submit(new ProgramStatusSubscriberThread(
      cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)));
  }

  private class ProgramStatusSubscriberThread
      extends AbstractNotificationSubscriberService.NotificationSubscriberThread {

    ProgramStatusSubscriberThread(String topic) {
      super(topic);
    }

    @Override
    protected String loadMessageId() {
      return null;
    }

    @Override
    protected void processNotification(DatasetContext context, Notification notification) throws Exception {
      // Required parameters
      Map<String, String> properties = notification.getProperties();
      String programRunIdString = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programStatusString = properties.get(ProgramOptionConstants.PROGRAM_STATUS);

      ProgramRunStatus programRunStatus = null;
      if (programStatusString != null) {
        try {
          programRunStatus = ProgramRunStatus.valueOf(programStatusString);
        } catch (IllegalArgumentException e) {
          LOG.warn("Invalid program status {} passed for program {}", programStatusString, programRunIdString, e);
          // Fall through, let the thread return normally
        }
      }

      // Ignore notifications which specify an invalid ProgramId, RunId, or ProgramRunStatus
      if (programRunIdString == null || programRunStatus == null) {
        return;
      }

      ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);
      Arguments userArguments = getArguments(properties, ProgramOptionConstants.USER_OVERRIDES);
      Arguments systemArguments = getArguments(properties, ProgramOptionConstants.SYSTEM_OVERRIDES);
      ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(cConf, messagingService)
        .withArguments(userArguments.asMap(), systemArguments.asMap());

      long startTime = getTime(notification.getProperties(), ProgramOptionConstants.LOGICAL_START_TIME);
      long endTime = getTime(notification.getProperties(), ProgramOptionConstants.END_TIME);
      System.out.println("PERSIST " + programRunId + " with Status " + programRunStatus + " START " + startTime);
      switch(programRunStatus) {
        case STARTING:
          if (startTime == -1) {
            LOG.debug("Start time not specified in notification for program id {}, not persisting" + programRunId);
            return;
          }
          programStateWriter.start(programRunId, twillRunId, startTime);
          break;
        case RUNNING:
          if (startTime == -1) {
            LOG.debug("Start time not specified in notification for program id {}, not persisting" + programRunId);
            return;
          }
          programStateWriter.running(programRunId, twillRunId, startTime);
          break;
        case COMPLETED:
        case SUSPENDED:
        case KILLED:
          if (endTime == -1) {
            LOG.debug("End time not specified in notification for program id {}, not persisting" + programRunId);
            return;
          }
          programStateWriter.stop(programRunId, endTime, programRunStatus, null);
          break;
        case FAILED:
          if (endTime == -1) {
            LOG.debug("End time not specified in notification for program id {}, not persisting" + programRunId);
            return;
          }
          BasicThrowable cause = GSON.fromJson(properties.get("error"), BasicThrowable.class);
          programStateWriter.stop(programRunId, endTime, ProgramRunStatus.FAILED, cause);
          break;
        default:
          throw new IllegalArgumentException(String.format("Cannot persist ProgramRunStatus %s for Program %s",
                                                           programRunStatus, programRunId));
      }

      if (programRunStatus != ProgramRunStatus.STARTING) {
        ProgramStatus programStatus = ProgramStatus.valueOf(programRunStatus.toString().toUpperCase());
        String triggerKeyForProgramStatus = Schedulers.triggerKeyForProgramStatus(programRunId.getParent(),
                                                                                  programStatus);

        if (canTriggerOtherPrograms(context, triggerKeyForProgramStatus)) {
          // Now send the notification to the scheduler
          TopicId programStatusTriggerTopic =
            NamespaceId.SYSTEM.topic(cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC));
          messagingService.publish(StoreRequestBuilder.of(programStatusTriggerTopic)
                  .addPayloads(GSON.toJson(notification))
                  .build());
        }
      }
    }

    private long getTime(Map<String, String> properties, String option) {
      String timeString = properties.get(option);
      return (timeString == null) ? -1 : Long.valueOf(timeString);
    }

    private Arguments getArguments(Map<String, String> properties, String option) {
      String argumentsString = properties.get(option);
      Map<String, String> arguments = GSON.fromJson(argumentsString, STRING_STRING_MAP);
      return (arguments == null) ? new BasicArguments()
                                 : new BasicArguments(arguments);
    }
  }

  private boolean canTriggerOtherPrograms(DatasetContext context, String triggerKey)
          throws IOException, DatasetManagementException {
    return !Schedulers.getScheduleStore(context, datasetFramework).findSchedules(triggerKey).isEmpty();
  }
}
