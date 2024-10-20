package com.tigerbeetle.client.dto;

import java.math.BigInteger;
import java.util.UUID;

public record TransferDto(UUID sourceAccount, UUID targetAccount, BigInteger amount, int ledger, int code,
                          UUID userData128, long userData64, int userData32) {
}