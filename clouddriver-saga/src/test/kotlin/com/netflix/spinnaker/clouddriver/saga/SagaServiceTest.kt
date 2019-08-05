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

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.clouddriver.event.EventMetadata
import com.netflix.spinnaker.clouddriver.event.SynchronousEventPublisher
import com.netflix.spinnaker.clouddriver.event.config.MemoryEventRepositoryConfigProperties
import com.netflix.spinnaker.clouddriver.event.persistence.EventRepository
import com.netflix.spinnaker.clouddriver.event.persistence.MemoryEventRepository
import com.netflix.spinnaker.clouddriver.saga.exceptions.SagaSystemException
import com.netflix.spinnaker.clouddriver.saga.models.Saga
import com.netflix.spinnaker.clouddriver.saga.persistence.DefaultSagaRepository
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.every
import io.mockk.mockk
import org.springframework.context.ApplicationContext
import strikt.api.expectThrows

class SagaServiceTest : JUnit5Minutests {

  fun tests() = rootContext<Fixture> {

    fixture {
      Fixture()
    }

    context("applying a non-existent saga") {
      test("throws system exception") {
        val saga = Saga(
          name = "noexist",
          id = "nope",
          completionHandler = null,
          requiredEvents = listOf(),
          compensationEvents = listOf()
        )
        expectThrows<SagaSystemException> {
          subject.apply(EmptyEvent(saga))
        }
      }
    }

    context("applying an event to a saga") {
      test("no matching event handler does nothing") {
        val saga = Saga(
          name = "test",
          id = "1",
          completionHandler = null,
          requiredEvents = listOf(),
          compensationEvents = listOf()
        )

        every { handlerProvider.getMatching(any(), any()) } returns listOf()

        subject.save(saga)
        subject.apply(EmptyEvent(saga).apply {
          metadata = EventMetadata(sequence = 0, originatingVersion = 0)
        })
      }
    }
  }

  inner class Fixture {
    val eventPublisher: SynchronousEventPublisher = SynchronousEventPublisher()

    val eventRepository: EventRepository = MemoryEventRepository(
      MemoryEventRepositoryConfigProperties(),
      eventPublisher,
      NoopRegistry()
    )

    val handlerProvider: SagaEventHandlerProvider = mockk(relaxed = true)

    val subject: SagaService = SagaService(
      DefaultSagaRepository(eventRepository),
      eventRepository,
      handlerProvider,
      NoopRegistry()
    )

    val applicationContext: ApplicationContext = mockk(relaxed = true) {
      eventPublisher.setApplicationContext(this)
      subject.setApplicationContext(this)
    }
  }

  inner class EmptyEvent(saga: Saga) : SagaEvent(saga.name, saga.id)
}
