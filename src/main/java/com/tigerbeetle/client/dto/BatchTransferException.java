package com.tigerbeetle.client.dto;

import com.tigerbeetle.CreateTransferResultBatch;

import java.util.List;

public class BatchTransferException extends RuntimeException {

    private final List result;

    public BatchTransferException(List result) {
        this.result = result;
    }

    public List getResult() {
        return this.result;
    }
}
