package org.camunda.community.eze.db

import io.camunda.zeebe.db.DbKey
import org.agrona.DirectBuffer
import org.agrona.MutableDirectBuffer

/** This class is used only internally by #isEmpty to search for same column family prefix.  */
internal class DbNullKey : DbKey {
    override fun wrap(buffer: DirectBuffer, offset: Int, length: Int) {
        // do nothing
    }

    override fun write(buffer: MutableDirectBuffer, offset: Int) {
        // do nothing
    }

    override fun getLength(): Int {
        return 0
    }

    companion object {
        val INSTANCE = DbNullKey()
    }
}
