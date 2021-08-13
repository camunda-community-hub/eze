package org.camunda.community.eze.db

import io.camunda.zeebe.db.TransactionContext
import io.camunda.zeebe.db.TransactionOperation
import io.camunda.zeebe.db.ZeebeDbTransaction
import java.util.*


class EzeDbTransactionContext(val database : TreeMap<Bytes, Bytes>) : TransactionContext {
    private val transaction = EzeDbTransaction(database)

    override fun runInTransaction(operations: TransactionOperation) {
        try {
            if (transaction.isInCurrentTransaction()) {
                operations.run()
            } else {
                runInNewTransaction(operations)
            }
        } catch (ex: Exception) {
            throw RuntimeException(
                "Unexpected error occurred during zeebe db transaction operation.", ex
            )
        }
    }

    override fun getCurrentTransaction(): ZeebeDbTransaction {
        if (!transaction.isInCurrentTransaction()) {
            transaction.rollback()
        }
        return transaction
    }

    private fun runInNewTransaction(operations: TransactionOperation) {
        try {
            transaction.resetTransaction()
            operations.run()
            transaction.commit()
        } finally {
            transaction.rollback()
        }
    }
}
