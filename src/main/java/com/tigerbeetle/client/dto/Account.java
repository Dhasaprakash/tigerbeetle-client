package com.tigerbeetle.client.dto;

import lombok.Builder;

import java.math.BigInteger;
import java.util.UUID;

@Builder
public record Account(
    UUID id,
    BigInteger accountNumber,
    int code,
    int ledger,
    int userData32,
    long userData64,
    BigInteger creditsPosted,
    BigInteger creditsPending,
    BigInteger debtsPosted,
    BigInteger debtsPending,
    int flags,
    long timestamp) {}
