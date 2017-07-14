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

package co.cask.cdap.app.runtime;

import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramRunId;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A no-op {@link ProgramStateWriter}
 */
public final class NoOpProgramStateWriter implements ProgramStateWriter {

  @Override
  public void start(ProgramRunId programRunId, String twillRunId, long startTime) {
    // no-op
  }

  @Override
  public void running(ProgramRunId programRunId, String twillRunId, long startTime) {
    // no-op
  }

  @Override
  public void stop(ProgramRunId programRunId, long endTime, ProgramRunStatus runStatus,
                   @Nullable BasicThrowable cause) {
    // no-op
  }

  @Override
  public void suspend(ProgramRunId programRunId) {
    // no-op
  }

  @Override
  public void resume(ProgramRunId programRunId) {
    // no-op
  }

  @Override
  public ProgramStateWriter withArguments(Map<String, String> userArguments, Map<String, String> systemArguments) {
    return this;
  }
}
