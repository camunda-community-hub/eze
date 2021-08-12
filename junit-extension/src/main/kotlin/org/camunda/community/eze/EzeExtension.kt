import org.camunda.community.eze.EngineFactory
import org.camunda.community.eze.ZeebeEngine
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource
import org.junit.platform.commons.util.ExceptionUtils
import org.junit.platform.commons.util.ReflectionUtils
import java.lang.reflect.Field
import java.util.function.Consumer

class EzeExtension : BeforeEachCallback {

    override fun beforeEach(context: ExtensionContext?) {
        context?.requiredTestInstances?.allInstances?.forEach {
            injectFields(context, it, it.javaClass)
        }
    }

    private fun injectFields(context: ExtensionContext, testInstance: Any, testClass: Class<*>) {
        ReflectionUtils.findFields(
            testClass,
            { field: Field ->
                ReflectionUtils.isNotStatic(field) &&
                        field.type == ZeebeEngine::class.java
            },
            ReflectionUtils.HierarchyTraversalMode.TOP_DOWN
        )
            .forEach(
                Consumer { field: Field ->
                    try {
                        ReflectionUtils.makeAccessible(field)[testInstance] =
                            lookupOrCreate(context).zeebe
                    } catch (t: Throwable) {
                        ExceptionUtils.throwAsUncheckedException(t)
                    }
                })
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
