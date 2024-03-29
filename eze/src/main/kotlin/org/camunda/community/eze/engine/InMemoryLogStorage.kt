/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine

import io.camunda.zeebe.logstreams.storage.LogStorage
import io.camunda.zeebe.logstreams.storage.LogStorageReader
import io.camunda.zeebe.util.buffer.BufferUtil
import io.camunda.zeebe.util.buffer.BufferWriter
import org.agrona.DirectBuffer
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.Function

class InMemoryLogStorage : LogStorage {

    private val positionIndexMapping = ConcurrentSkipListMap<Long, Int>()
    private val entries = mutableListOf<Entry>()
    private val commitListeners = mutableSetOf<LogStorage.CommitListener>()

    override fun newReader(): LogStorageReader {
        return ListLogStorageReader()
    }

    override fun addCommitListener(listener: LogStorage.CommitListener) {
        commitListeners.add(listener)
    }

    override fun removeCommitListener(listener: LogStorage.CommitListener) {
        commitListeners.remove(listener)
    }

    override fun append(
        lowestPosition: Long,
        highestPosition: Long,
        bufferWriter: BufferWriter,
        listener: LogStorage.AppendListener
    ) {
        try {
            val copy: DirectBuffer = BufferUtil.createCopy(bufferWriter)

            val entry = Entry(copy)
            entries.add(entry)
            val index = entries.size
            positionIndexMapping[lowestPosition] = index
            listener.onWrite(index.toLong())

            listener.onCommit(index.toLong())
            commitListeners.forEach(LogStorage.CommitListener::onCommit)
        } catch (e: Exception) {
            listener.onWriteError(e)
        }
    }

    private data class Entry(val data: DirectBuffer)

    private inner class ListLogStorageReader : LogStorageReader {

        var currentIndex = 0

        override fun seek(position: Long) {
            currentIndex = Optional.ofNullable(positionIndexMapping.lowerEntry(position))
                .map(Function<Map.Entry<Long, Int>, Int> { it.value })
                .map { index: Int -> index - 1 }
                .orElse(0)
        }

        override fun close() {}

        override fun remove() {
            TODO("Not yet implemented")
        }

        override fun hasNext(): Boolean {
            return currentIndex >= 0 && currentIndex < entries.size
        }

        override fun next(): DirectBuffer {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            val index = currentIndex
            currentIndex++
            return entries[index].data
        }


    }
}
