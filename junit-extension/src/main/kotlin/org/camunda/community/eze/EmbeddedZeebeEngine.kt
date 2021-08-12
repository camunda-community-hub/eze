package org.camunda.community.eze

import EzeExtension
import org.junit.jupiter.api.extension.ExtendWith

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(EzeExtension::class)
annotation class EmbeddedZeebeEngine()
