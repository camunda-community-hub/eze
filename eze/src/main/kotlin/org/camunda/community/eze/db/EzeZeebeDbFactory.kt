/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.db

import io.camunda.zeebe.db.ZeebeDbFactory
import io.camunda.zeebe.protocol.EnumValue
import org.camunda.community.eze.db.EzeDbFactory.Companion.newFactory

object EzeZeebeDbFactory {
    @JvmStatic
    fun <ColumnFamilyType> getDefaultFactory(): ZeebeDbFactory<ColumnFamilyType>
            where ColumnFamilyType : Enum<ColumnFamilyType>,
                  ColumnFamilyType : EnumValue {
        return newFactory()
    }
}
