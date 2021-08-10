[![Community badge: Incubating](https://img.shields.io/badge/Lifecycle-Incubating-blue)](https://github.com/Camunda-Community-Hub/community/blob/main/extension-lifecycle.md#incubating-)
[![Community extension badge](https://img.shields.io/badge/Community%20Extension-An%20open%20source%20community%20maintained%20project-FF4700)](https://github.com/camunda-community-hub/community)

# Embedded Zeebe engine

Was created as part of the Summer Hackdays 2021.

## Why:

We run on most projects' test containers, to test our code. The problem is that it takes a long time to execute and set up (~15s). Execution time is quite low. It is a blackbox and hard to debug and understand what is going on.

Adjustment of time is not easily possible with test containers, with the embedded engine it should work out of the box. (https://github.com/camunda-community-hub/zeebe-test-container/issues/74) 

It is currently hard with test containers to get the records out, which should be easier with an embedded engine.

For several projects we want to have a library which we can just plug in and use to test the code we wrote.

## Goals:

1. An library / embedded engine which can be used in our other side projects (Kotlin/Java)
2. It should be possible to use normal clients to interact with the engine
3. The engine should be usable with an in memory mode but should also support a persisted mode
4. Time travel should be easily possible in order to make engine / bpmn tests easier
5. Produced records should be accessible via an API

## Env

Written in kotlin and java (11).

