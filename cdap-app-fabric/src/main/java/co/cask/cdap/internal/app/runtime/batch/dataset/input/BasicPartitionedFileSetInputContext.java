/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import co.cask.cdap.api.data.batch.PartitionedFileSetInputContext;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionKeyCodec;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A basic implementation of {@link PartitionedFileSetInputContext}.
 */
class BasicPartitionedFileSetInputContext extends BasicInputContext implements PartitionedFileSetInputContext {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(PartitionKey.class, new PartitionKeyCodec()).create();

  private static final Type STRING_PARTITION_KEY_MAP_TYPE = new TypeToken<Map<String, PartitionKey>>() { }.getType();

  private final Map<String, PartitionKey> pathToPartitionMapping;

  private final boolean isCombineInputFormat;
  private final Configuration conf;

  private final Supplier<Path[]> inputPaths;
  private List<PartitionKey> partitionKeys;

  // for caching in case of CombineFileInputFormat
  private String currentInputfileName;
  private PartitionKey currentPartitionKey;

  BasicPartitionedFileSetInputContext(MultiInputTaggedSplit multiInputTaggedSplit) {
    super(multiInputTaggedSplit.getName());

    InputSplit inputSplit = multiInputTaggedSplit.getInputSplit();
    if (inputSplit instanceof FileSplit) {
      isCombineInputFormat = false;
      Path path = ((FileSplit) inputSplit).getPath();
      inputPaths = Suppliers.ofInstance(new Path[] { path });
    } else if (inputSplit instanceof CombineFileSplit) {
      isCombineInputFormat = true;
      inputPaths = Suppliers.ofInstance(((CombineFileSplit) inputSplit).getPaths());
    } else {
      throw new IllegalArgumentException(String.format("Expected either a '%s' or a '%s', but got '%s'.",
                                                       FileSplit.class.getName(), CombineFileSplit.class.getName(),
                                                       inputSplit.getClass().getName()));
    }

    this.conf = multiInputTaggedSplit.getConf();
    String mappingString = conf.get(PartitionedFileSetDataset.PATH_TO_PARTITIONING_MAPPING);
    this.pathToPartitionMapping =
      GSON.fromJson(Objects.requireNonNull(mappingString), STRING_PARTITION_KEY_MAP_TYPE);
  }

  @Override
  public PartitionKey getInputPartitionKey() {
    if (isCombineInputFormat) {
      // org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader sets this in its initNextRecordReader method
      String inputFileName = conf.get(MRJobConfig.MAP_INPUT_FILE);
      Preconditions.checkNotNull(inputFileName);
      if (!inputFileName.equals(currentInputfileName)) {
        currentPartitionKey = getPartitionKey(URI.create(inputFileName));
        currentInputfileName = inputFileName;
      }
      return currentPartitionKey;
    }

    // single split per mapper task
    List<PartitionKey> inputPartitionKeys = getInputPartitionKeys();
    if (inputPartitionKeys.size() != 1) {
      throw new IllegalStateException(String.format("Expected a single PartitionKey, but found: %s",
                                                    inputPartitionKeys));
    }
    return inputPartitionKeys.get(0);
  }

  @Override
  public List<PartitionKey> getInputPartitionKeys() {
    if (partitionKeys == null) {
      partitionKeys = new ArrayList<>(inputPaths.get().length);
      for (Path inputPath : inputPaths.get()) {
        partitionKeys.add(getPartitionKey(inputPath.toUri()));
      }
    }
    return partitionKeys;
  }

  private PartitionKey getPartitionKey(URI inputPathURI) {
    if (pathToPartitionMapping.containsKey(inputPathURI.toString())) {
      return pathToPartitionMapping.get(inputPathURI.toString());
    }
    for (Map.Entry<String, PartitionKey> pathEntry : pathToPartitionMapping.entrySet()) {
      if (isParentOrEquals(URI.create(pathEntry.getKey()), inputPathURI)) {
        return pathEntry.getValue();
      }
    }
    throw new IllegalArgumentException(
      String.format("Failed to derive PartitionKey from input path '%s' and path to key mapping '%s'.",
                    inputPathURI, pathToPartitionMapping));
  }

  // compares only the paths of the URI, ignoring the scheme, host, port, etc. of the URIs.
  private boolean isParentOrEquals(URI potentialParent, URI potentialChild) {
    return potentialChild.normalize().getPath().startsWith(potentialParent.normalize().getPath());
  }
}
