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
package com.netflix.spinnaker.clouddriver.saga

import com.google.common.annotations.Beta
import com.netflix.spinnaker.clouddriver.saga.models.Saga

interface SagaEventHandler<T : SagaEvent> {
  fun apply(event: T, saga: Saga): List<SagaEvent>
  fun compensate(event: T, saga: Saga) {}
}

/**
 * TODO(rz): Thinking about changing to this interface instead. Only allow emitting a single event instead to make
 * things a little simpler on logic.
 */
@Beta
private interface SagaEventHandler2<A : SagaEvent, B : SagaEvent, C : SagaRollbackEvent> {
  fun apply(event: A, saga: Saga): B
  fun rollback(event: B, saga: Saga): C
}
private abstract class SagaRollbackEvent(sagaName: String, sagaId: String) : SagaEvent(sagaName, sagaId)
