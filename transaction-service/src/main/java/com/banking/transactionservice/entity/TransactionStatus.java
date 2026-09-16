package com.banking.transactionservice.entity;

    /*
    Transaction Lifecycle Flow:

     PENDING -> PROCESSING -> COMPLETED (clean transaction)
     PENDING -> PROCESSING -> PENDING_VERIFICATION (suspicious detected)
                                -> COMPLETED (verified)
                                -> FLAGGED (SAGA REFUND)
     PENDING -> PROCESSING -> FAILED
     PENDING -> PROCESSING -> FLAGGED
     */

public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}