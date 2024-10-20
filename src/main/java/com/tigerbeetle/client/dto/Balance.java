package com.tigerbeetle.client.dto;

import lombok.Builder;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

@Builder
public record Balance(
  UUID accountId,
  String timestamp,
  BigInteger creditsPosted,
  BigInteger creditsPending,
  BigInteger debitsPosted,
  BigInteger debitsPending
) {
}
