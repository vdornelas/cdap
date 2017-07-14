/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.data.schema.Schema;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Exposes read-only lookup operations on datasets.
 *
 * @param <T> the type of object that will be returned for a lookup with String as the key
 * @param <T> the raw type of object that will be returned for lookups with byte[] as the key
 */
public interface Lookup<T,R> {

  /**
   * @return Schema of the dataset, or null if no schema is stored in the dataset's properties
   */
  @Nullable
  Schema getSchema();

  /**
   * Performs a single lookup.
   *
   * @param key the key to lookup
   * @return the value associated with the key
   */
  R lookup(byte[] key);

  /**
   * Performs a batch lookup.
   *
   * @param keys the keys to lookup
   * @return a map from key to value
   */
  Map<byte[], R> lookup(byte[]... keys);

  /**
   * Performs a single lookup.
   *
   * @param key the key to lookup
   * @return the value associated with the key
   */
  T lookup(String key);

  /**
   * Performs a batch lookup.
   *
   * @param keys the keys to lookup
   * @return a map from key to value
   */
  Map<String, T> lookup(String... keys);

  /**
   * Performs a batch lookup.
   *
   * @param keys the keys to lookup
   * @return a map from key to value
   */
  Map<String, T> lookup(Set<String> keys);
}
