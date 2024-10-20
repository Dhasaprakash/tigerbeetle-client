package com.tigerbeetle.client.dto;

import com.tigerbeetle.CreateTransferResult;
import com.tigerbeetle.CreateTransferResultBatch;

public class TransferException extends RuntimeException {

    private final CreateTransferResult result;

    public TransferException(CreateTransferResult result) {
        this.result = result;
    }

    public CreateTransferResult getResult() {
        return this.result;
    }
}
