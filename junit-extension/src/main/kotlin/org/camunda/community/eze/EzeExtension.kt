/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
import io.camunda.zeebe.client.ZeebeClient
import org.camunda.community.eze.EngineFactory
import org.camunda.community.eze.RecordStream.print
import org.camunda.community.eze.RecordStreamSource
import org.camunda.community.eze.ZeebeEngine
import org.camunda.community.eze.ZeebeEngineClock
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource
import org.junit.jupiter.api.extension.TestWatcher
import org.junit.platform.commons.util.ExceptionUtils
import org.junit.platform.commons.util.ReflectionUtils
import java.lang.reflect.Field
import kotlin.reflect.KClass

class EzeExtension : BeforeEachCallback, TestWatcher {

    override fun beforeEach(context: ExtensionContext?) {
        context?.requiredTestInstances?.allInstances?.forEach {
            injectFields(context, it, it.javaClass)
        }
    }

    override fun testFailed(context: ExtensionContext?, cause: Throwable?) {
        context?.let {
            println("===== Test failed! Printing records from the log stream =====")
            lookupOrCreate(it).zeebe.records().print(compact = true)
            println("----------")
        }
    }

    private fun injectFields(context: ExtensionContext, testInstance: Any, testClass: Class<*>) {
        mapOf(
            ZeebeEngine::class to EzeExtensionState::zeebe,
            ZeebeClient::class to { it.zeebe.createClient() },
            ZeebeEngineClock::class to { it.zeebe.clock() },
            RecordStreamSource::class to EzeExtensionState::zeebe
        ).forEach { (fieldType, fieldValue) ->
            injectFields(context, testInstance, testClass, fieldType, fieldValue)
        }
    }

    private fun injectFields(
        context: ExtensionContext,
        testInstance: Any,
        testClass: Class<*>,
        fieldType: KClass<*>,
        fieldValue: (EzeExtensionState) -> Any
    ) {
        val fields = ReflectionUtils.findFields(
            testClass,
            { field: Field -> ReflectionUtils.isNotStatic(field) && field.type == fieldType.java },
            ReflectionUtils.HierarchyTraversalMode.TOP_DOWN
        )

        fields.forEach { field: Field ->
            try {
                ReflectionUtils.makeAccessible(field)[testInstance] =
                    fieldValue(lookupOrCreate(context))
            } catch (t: Throwable) {
                ExceptionUtils.throwAsUncheckedException(t)
            }
        }
    }

    private fun lookupOrCreate(context: ExtensionContext): EzeExtensionState {
        return getStore(context).getOrComputeIfAbsent(
            "zeebe",
            {
                EzeExtensionState(
                    zeebe = EngineFactory.create()
                )
            }) as EzeExtensionState
    }

    private fun getStore(context: ExtensionContext): ExtensionContext.Store {
        return context.getStore(ExtensionContext.Namespace.create(javaClass, context.uniqueId))
    }

    class EzeExtensionState(val zeebe: ZeebeEngine) : CloseableResource {
        init {
            zeebe.start()
        }

        override fun close() {
            zeebe.stop()
        }
    }

}
