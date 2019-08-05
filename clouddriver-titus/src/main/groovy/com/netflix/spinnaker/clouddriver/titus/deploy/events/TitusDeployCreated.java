/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.clouddriver.titus.deploy.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.hash.Hashing;
import com.netflix.spinnaker.clouddriver.saga.SagaEvent;
import com.netflix.spinnaker.clouddriver.titus.TitusException;
import com.netflix.spinnaker.clouddriver.titus.deploy.description.TitusDeployDescription;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

@Getter
public class TitusDeployCreated extends SagaEvent {

  private static final ObjectMapper CHECKSUM_MAPPER =
      new ObjectMapper()
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
          .disable(SerializationFeature.INDENT_OUTPUT);

  @Nonnull private final TitusDeployDescription description;
  @Nonnull private final List priorOutputs;
  @Nonnull private final String inputChecksum;

  public TitusDeployCreated(
      @NotNull String sagaName,
      @NotNull String sagaId,
      @Nonnull TitusDeployDescription description,
      @Nonnull List priorOutputs) {
    super(sagaName, sagaId);
    this.description = description;
    this.priorOutputs = priorOutputs;
    this.inputChecksum = checksum(description);
  }

  private static String checksum(TitusDeployDescription description) {
    try {
      return Hashing.murmur3_32()
          .hashBytes(CHECKSUM_MAPPER.writeValueAsBytes(description))
          .toString();
    } catch (JsonProcessingException e) {
      throw new TitusException("Failed creating checksum for TitusDeployDescription", e);
    }
  }
}
