package com.tigerbeetle.client.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.UUID;

@Builder
//@Getter
//@Setter
public record Filters (
    UUID accountId,
    boolean credits,
    boolean debits,
    Date fromDate,
    Date toDate,
    int limit
)
{}
