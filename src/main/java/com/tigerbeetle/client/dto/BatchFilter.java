package com.tigerbeetle.client.dto;

import java.util.Date;
import java.util.UUID;

public record BatchFilter(
        UUID accountNumber,
        long userData64,
        int userData32,
        int code,
        int ledger,
        Date fromDate,
        Date toDate,
        int limit,
        boolean reversed
) {
}
