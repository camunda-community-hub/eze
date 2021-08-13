/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
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
