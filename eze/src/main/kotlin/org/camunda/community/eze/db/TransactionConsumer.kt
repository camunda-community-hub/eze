package org.camunda.community.eze.db

internal fun interface TransactionConsumer {
    /**
     * Consumes a transaction, in order to make sure that a transaction is open.
     *
     * @param transaction the to consumed transaction
     * @throws Exception if an unexpected exception occurs, on opening a new transaction for example
     */
    @Throws(Exception::class)
    fun run(transaction: EzeDbTransaction)
}
