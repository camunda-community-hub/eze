package org.camunda.community.eze.engine

import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue
import io.camunda.zeebe.stream.api.records.TypedRecord
import io.camunda.zeebe.stream.api.scheduling.ProcessingScheduleService
import io.camunda.zeebe.stream.api.scheduling.Task
import io.camunda.zeebe.stream.impl.records.RecordBatchEntry
import org.camunda.community.eze.records.RecordWrapper
import org.camunda.community.eze.records.RecordsList
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class SingleThreadProcessingScheduleService(private val records: RecordsList) :
    ProcessingScheduleService {

    private val executor = Executors.newSingleThreadScheduledExecutor()

    override fun runDelayed(delay: Duration?, task: Runnable?) {
        executor.schedule(task!!, delay!!.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun runDelayed(delay: Duration?, task: Task?) {
        executor.schedule({ executeTask(task!!)}, delay!!.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun runAtFixedRate(delay: Duration?, task: Task?) {
        executor.scheduleAtFixedRate({ executeTask(task!!)}, delay!!.toMillis(), delay.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun executeTask(task: Task) {
        val taskResultBuilder = BufferedTaskResultBuilder({ _, _ -> true })

        try {
            val taskResult = task.execute(taskResultBuilder)

            val intermediateList = mutableListOf<TypedRecord<UnifiedRecordValue>>()
            for (record: RecordBatchEntry in taskResult.recordBatch) {
                val typedRecord = RecordWrapper.convert(record)
                intermediateList.add(typedRecord)
            }
            // in order to add a batch of records (atomically) we need to addAll
            records.addAll(intermediateList)
        } catch (e: Exception) {
            // well idk
        }

    }

}
