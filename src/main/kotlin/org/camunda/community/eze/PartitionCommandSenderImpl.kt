package org.camunda.community.eze

import io.camunda.zeebe.engine.processing.message.command.PartitionCommandSender
import io.camunda.zeebe.util.buffer.BufferWriter
import org.agrona.concurrent.UnsafeBuffer

class PartitionCommandSenderImpl(private val subscriptionHandler: (Int, ByteArray) -> Unit) :
    PartitionCommandSender {

    override fun sendCommand(receiverPartitionId: Int, command: BufferWriter): Boolean {

        val bytes = ByteArray(command.length)
        val commandBuffer = UnsafeBuffer(bytes)
        command.write(commandBuffer, 0)

        // delegate the command to the subscription handler of the receiver partition
        subscriptionHandler(receiverPartitionId, bytes)

        return true
    }
}
