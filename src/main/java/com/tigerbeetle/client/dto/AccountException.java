package com.tigerbeetle.client.dto;

import com.tigerbeetle.CreateAccountResult;

public class AccountException extends RuntimeException{

    private final CreateAccountResult result;

    public AccountException(CreateAccountResult result) {
        this.result = result;
    }

    public CreateAccountResult getResult() {
        return this.result;
    }
}
