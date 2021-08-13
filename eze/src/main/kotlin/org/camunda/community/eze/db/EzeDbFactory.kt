/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.db

import io.camunda.zeebe.db.ZeebeDb
import io.camunda.zeebe.db.ZeebeDbFactory
import io.camunda.zeebe.engine.state.ZbColumnFamilies
import java.io.File

class EzeDbFactory<ColumnFamilyType : Enum<ColumnFamilyType>> : ZeebeDbFactory<ColumnFamilyType> {

    companion object {
        fun <ColumnFamilyType : Enum<ColumnFamilyType>> newFactory() : ZeebeDbFactory<ColumnFamilyType>{
            return EzeDbFactory()
        }
    }

    override fun createDb(_ignore: File?): ZeebeDb<ColumnFamilyType> {
        return EzeDb()
    }
}
