package org.camunda.community.eze

import java.time.Duration

interface ZeebeEngineClock {
    fun increaseTime(timeToAdd: Duration)
}
