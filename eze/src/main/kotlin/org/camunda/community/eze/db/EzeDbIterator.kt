package org.camunda.community.eze.db

import java.util.*

class EzeDbIterator(val database : SortedMap<Bytes, Bytes>) {


    fun seek(prefix: ByteArray, prefixLength: Int) : EzeDbIterator {
        return EzeDbIterator(database.tailMap(prefix.toBytes(prefixLength)))
    }

    fun iterate() : MutableIterator<MutableMap.MutableEntry<Bytes, Bytes>> {
        return database.iterator()
    }

}
