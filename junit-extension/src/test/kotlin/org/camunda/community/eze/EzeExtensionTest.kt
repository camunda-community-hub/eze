package org.camunda.community.eze

import org.junit.jupiter.api.Test

@EmbeddedZeebeEngine
class EzeExtensionTest {

    lateinit var zeebe: ZeebeEngine

    @Test
    fun `should create client`() {
        val client = zeebe.createClient()

        client.newTopologyRequest().send().join()
    }

}
