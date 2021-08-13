package org.camunda.community.eze

import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.model.bpmn.Bpmn
import io.camunda.zeebe.protocol.record.intent.TimerIntent
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.camunda.community.eze.RecordStream.intent
import org.camunda.community.eze.RecordStream.timer
import org.junit.jupiter.api.Test
import java.time.Duration

@EmbeddedZeebeEngine
class EzeExtensionTest {

    lateinit var zeebe: ZeebeEngine
    lateinit var client: ZeebeClient
    lateinit var clock: ZeebeEngineClock

    @Test
    fun `should inject engine`() {
        // given
        assertThat(zeebe).isNotNull

        val client = zeebe.createClient()

        // when
        val result = client.newDeployCommand()
            .addProcessModel(process, "process.bpmn")
            .send()
            .join()

        // then
        assertThat(result.processes).hasSize(1)
    }

    @Test
    fun `should inject client`() {
        // given
        assertThat(client).isNotNull

        // when
        val result = client.newDeployCommand()
            .addProcessModel(process, "process.bpmn")
            .send()
            .join()

        // then
        assertThat(result.processes).hasSize(1)
    }

    @Test
    fun `should inject clock`() {
        // given
        assertThat(clock).isNotNull

        client.newDeployCommand()
            .addProcessModel(timerProcess, "process.bpmn")
            .send()
            .join()

        client.newCreateInstanceCommand()
            .bpmnProcessId("process")
            .latestVersion()
            .send()
            .join()

        await.untilAsserted {
            val timerCreated = zeebe.records()
                .timer()
                .intent(TimerIntent.CREATED)
                .firstOrNull()

            assertThat(timerCreated).isNotNull
        }

        // when
        clock.increaseTime(Duration.ofDays(1))

        // then
        await.untilAsserted {
            val timerTriggered = zeebe.records()
                .timer()
                .intent(TimerIntent.TRIGGERED)
                .firstOrNull()

            assertThat(timerTriggered).isNotNull
        }
    }

    companion object {
        val process = Bpmn.createExecutableProcess("process")
            .startEvent()
            .endEvent()
            .done()

        val timerProcess = Bpmn.createExecutableProcess("process")
            .startEvent()
            .intermediateCatchEvent()
            .timerWithDuration("P1D")
            .endEvent()
            .done()
    }

}
