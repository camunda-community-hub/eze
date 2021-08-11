package org.camunda.community.eze

import io.camunda.zeebe.logstreams.storage.LogStorage
import java.util.concurrent.ConcurrentNavigableMap
import java.util.function.LongConsumer
import io.camunda.zeebe.logstreams.storage.LogStorageReader
import org.agrona.DirectBuffer
import org.agrona.concurrent.UnsafeBuffer
import java.lang.Exception
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
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
        blockBuffer: ByteBuffer,
        listener: LogStorage.AppendListener
    ) {
        try {
            val entry = Entry(blockBuffer)
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

    private data class Entry(val data: ByteBuffer)

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
            return UnsafeBuffer(entries[index].data)
        }


    }
}
