package org.camunda.community.eze

import io.camunda.zeebe.util.sched.clock.ControlledActorClock
import java.time.Duration

class ZeebeEngineClockImpl(val clock: ControlledActorClock) : ZeebeEngineClock {

    override fun increaseTime(timeToAdd: Duration) {
        clock.addTime(timeToAdd)
    }
}
