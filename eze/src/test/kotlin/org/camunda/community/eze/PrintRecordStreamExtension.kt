package org.camunda.community.eze

import org.camunda.community.eze.RecordStream.print
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.TestWatcher

class PrintRecordStreamExtension : TestWatcher {

    override fun testFailed(context: ExtensionContext?, cause: Throwable?) {
        println("===== Test failed! Printing records from the stream:")
        zeebeEngine.records().print(compact = true)
    }

    companion object {
        lateinit var zeebeEngine: ZeebeEngine
    }

}
