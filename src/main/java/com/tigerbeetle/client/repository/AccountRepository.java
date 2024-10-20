package com.tigerbeetle.client.repository;

import com.tigerbeetle.client.dto.*;
import com.tigerbeetle.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@RequiredArgsConstructor
public class AccountRepository {
    private final Client client;

    public List<Account> createAccountBatch(List<Account> accounts){
        List accountList = new ArrayList();
        List<UUID> ids = new ArrayList<>();
        AccountBatch batch = new AccountBatch(accounts.size());
        for (Account account : accounts) {
            batch.add();
            byte[] id = UInt128.id();
            UUID uuid = UInt128.asUUID(id);
            ids.add(uuid);
            batch.setId(id);
            batch.setUserData128(UInt128.asBytes(account.accountNumber()));
            batch.setLedger(account.ledger());
            batch.setCode(account.code());
            batch.setUserData32(account.userData32());
            batch.setUserData64(account.userData64());
            batch.setFlags((batch.getPosition() + 1 == accounts.size()) ? AccountFlags.HISTORY : AccountFlags.HISTORY | AccountFlags.LINKED);

        }
        CreateAccountResultBatch result = client.createAccounts(batch);
        if (result.getLength() > 0) {
            result.next();
            throw new AccountException(result.getResult());
        }

        return findAccountsById(ids.toArray(new UUID[0])).values().stream().toList();
    }

    public List<Transfer> createBatchTransfer(List<Transfer> transfers)  {

        var results = new ArrayList<Map.Entry<UUID, CreateTransferResult>>(transfers.size());
        var batch = new TransferBatch(transfers.size());
        List<UUID> ids = new ArrayList<>();
        for (Transfer t : transfers) {
            byte[] id = UInt128.id();
            batch.add();
            batch.setId(id);
            UUID uuid = UInt128.asUUID(id);
            ids.add(uuid);
            // Is this the last transfer to add ?
            if (batch.getPosition() != transfers.size() - 1) {
                batch.setFlags(TransferFlags.LINKED);
            }

            batch.setLedger(t.ledger());
            batch.setAmount(t.amount());
            batch.setDebitAccountId(UInt128.asBytes(t.debitAccountId()));
            batch.setCreditAccountId(UInt128.asBytes(t.creditAccountId()));
            if (t.userData128() != null) {
                batch.setUserData128(UInt128.asBytes(t.userData128()));
            }
            batch.setUserData64(t.userData64());
            batch.setUserData32(t.userData32());
            batch.setCode(t.code());
            results.add(new AbstractMap.SimpleImmutableEntry<>(UInt128.asUUID(id), CreateTransferResult.Ok));
        }

        var batchResult = client.createTransfers(batch);
        boolean isException = false;
        while (batchResult.next()) {
            var original = results.get(batchResult.getIndex());
            results.set(batchResult.getIndex(), new AbstractMap.SimpleImmutableEntry<>(original.getKey(), batchResult.getResult()));
        }

        if(batchResult.getLength() > 0) {
            throw new BatchTransferException(results);
        }
        return findTransfersByBatchIds(ids).values().stream().toList();
    }

    public Map<UUID, Transfer> findTransfersByBatchIds(List<UUID> ids)  {
        IdBatch idBatch = new IdBatch(ids.size());
        Map<UUID, Transfer> result = new HashMap<>();
        for (UUID id : ids) {
            idBatch.add(UInt128.asBytes(id));
            result.put(id, null);
        }
        var batch = client.lookupTransfers(idBatch);
        while (batch.next()) {
            UUID key = UInt128.asUUID(batch.getId());
            Transfer transfer = mapFromCurrentTransferBatch(batch);
            result.put(key, transfer);
        }

        return result;
    }

    private Transfer mapFromCurrentTransferBatch(TransferBatch batch) {
        return Transfer.builder()
                .id(UInt128.asUUID(batch.getId()))
                .code(batch.getCode())
                .amount(batch.getAmount())
                .flags(batch.getFlags())
                .ledger(batch.getLedger())
                .creditAccountId(UInt128.asUUID(batch.getCreditAccountId()))
                .debitAccountId(UInt128.asUUID(batch.getDebitAccountId()))
                .userData128(UInt128.asUUID(batch.getUserData128()))
                .userData64(batch.getUserData64())
                .userData32(batch.getUserData32())
                .timestamp(convertTigerBeetleTimestampToDateTime(batch.getTimestamp()))
                .pendingId(UInt128.asUUID(batch.getPendingId()))
                .build();
    }

    public Account createAccount(BigInteger accountHolderId, int code, int ledger, int userData32, long userData64, int flags)  {

        AccountBatch batch = new AccountBatch(1);
        byte[] id = UInt128.id();
        batch.add();
        batch.setId(id);
        batch.setUserData128(UInt128.asBytes(accountHolderId));
        batch.setLedger(ledger);
        batch.setCode(code);
        batch.setUserData32(userData32);
        batch.setUserData64(userData64);
        batch.setFlags(AccountFlags.HISTORY | flags);

        CreateAccountResultBatch result = client.createAccounts(batch);
        if (result.getLength() > 0) {
            result.next();
            throw new AccountException(result.getResult());
        }
        return findAccountById(UInt128.asUUID(id)).orElseThrow();
    }

    public Optional<Account> findAccountById(UUID id)  {
        IdBatch idBatch = new IdBatch(UInt128.asBytes(id));
        var batch = client.lookupAccounts(idBatch);

        if (!batch.next()) {
            return Optional.empty();
        }

        return Optional.of(mapFromCurrentAccountBatch(batch));
    }

    public Transfer findTransfersById(UUID id)  {
        return findTransfersByBatchIds(List.of(id)).values().stream().toList().get(0);
    }

    public Map<UUID, Account> findAccountsById(UUID[] ids)  {

        IdBatch idBatch = new IdBatch(ids.length);
        Map<UUID, Account> result = new HashMap<>();
        for (UUID id : ids) {
            idBatch.add(UInt128.asBytes(id));
            result.put(id, null);
        }

        var batch = client.lookupAccounts(idBatch);
        while (batch.next()) {
            UUID key = UInt128.asUUID(batch.getId());
            Account acc = mapFromCurrentAccountBatch(batch);
            result.put(key, acc);
        }

        return result;

    }

    private Account mapFromCurrentAccountBatch(AccountBatch batch) {
        return Account.builder()
                .id(UInt128.asUUID(batch.getId()))
                .accountNumber(UInt128.asBigInteger(batch.getUserData128()))
                .flags(batch.getFlags())
                .code(batch.getCode())
                .ledger(batch.getLedger())
                .userData32(batch.getUserData32())
                .userData64(batch.getUserData64())
                .timestamp(batch.getTimestamp())
                .creditsPosted(batch.getCreditsPosted())
                .creditsPending(batch.getCreditsPending())
                .debtsPending(batch.getDebitsPending())
                .debtsPosted(batch.getDebitsPosted())
                .build();
    }

    public UUID createSimpleTransfer(TransferDto transferDto)  {

        var id = UInt128.id();
        var batch = new TransferBatch(1);

        batch.add();
        batch.setId(id);
        batch.setAmount(transferDto.amount());
        batch.setCode(transferDto.code());
        batch.setCreditAccountId(UInt128.asBytes(transferDto.targetAccount()));
        batch.setDebitAccountId(UInt128.asBytes(transferDto.sourceAccount()));
        batch.setUserData32(transferDto.userData32());
        batch.setUserData64(transferDto.userData64());
        batch.setUserData128(UInt128.asBytes(transferDto.userData128()));
        batch.setLedger(transferDto.ledger());

        var batchResults = client.createTransfers(batch);

        if (batchResults.getLength() > 0) {
            batchResults.next();
            throw new TransferException(batchResults.getResult());
        }
        return UInt128.asUUID(id);
    }

    public Transfer createPendingTransfer(Transfer transfer)  {

        var id = UInt128.id();
        var batch = new TransferBatch(1);

        batch.add();
        batch.setId(id);
        batch.setAmount(transfer.amount());
        batch.setCode(transfer.code());
        batch.setCreditAccountId(UInt128.asBytes(transfer.creditAccountId()));
        batch.setDebitAccountId(UInt128.asBytes(transfer.debitAccountId()));
        batch.setFlags(TransferFlags.PENDING);
        batch.setUserData32(transfer.userData32());
        batch.setUserData64(transfer.userData64());
        batch.setUserData128(UInt128.asBytes(transfer.userData128()));
        batch.setLedger(transfer.ledger());

        var batchResults = client.createTransfers(batch);

        if (batchResults.getLength() > 0) {
            batchResults.next();
            throw new TransferException(batchResults.getResult());
        }
        return findTransfersByBatchIds(List.of(UInt128.asUUID(id))).values().stream().toList().get(0);
    }

    public UUID createExpirablePendingTransfer(UUID sourceAccount, UUID targetAccount, BigInteger amount, int ledger, int code, UUID userData128, long userData64, int userData32, int timeout)  {

        var id = UInt128.id();
        var batch = new TransferBatch(1);

        batch.add();
        batch.setId(id);
        batch.setAmount(amount);
        batch.setCode(code);
        batch.setCreditAccountId(UInt128.asBytes(targetAccount));
        batch.setDebitAccountId(UInt128.asBytes(sourceAccount));
        batch.setFlags(TransferFlags.PENDING);
        batch.setUserData32(userData32);
        batch.setUserData64(userData64);
        if (userData128 != null) {
            batch.setUserData128(UInt128.asBytes(userData128));
        }
        batch.setLedger(ledger);
        batch.setTimeout(timeout);

        var batchResults = client.createTransfers(batch);

        if (batchResults.getLength() > 0) {
            batchResults.next();
            throw new TransferException(batchResults.getResult());
        }
        return UInt128.asUUID(id);
    }

    public Transfer completePendingTransfer(Transfer transfer)  {

        var id = UInt128.id();
        var batch = new TransferBatch(1);
        batch.add();
        batch.setId(id);
        batch.setPendingId(UInt128.asBytes(transfer.pendingId()));
//        batch.setDebitAccountId(UInt128.asBytes(transfer.debitAccountId()));
//        batch.setCreditAccountId(UInt128.asBytes(transfer.creditAccountId()));
//        batch.setLedger(transfer.ledger());
//        batch.setCode(transfer.code());
        batch.setAmount(transfer.amount());
        batch.setFlags(TransferFlags.POST_PENDING_TRANSFER);
//        batch.add();
////        batch.setId(id);
////        batch.setDebitAccountId(UInt128.asBytes(transfer.debitAccountId()));
////        batch.setCreditAccountId(UInt128.asBytes(transfer.creditAccountId()));
//        batch.setId(UInt128.asBytes(transfer.id()));
//        batch.setFlags(TransferFlags.POST_PENDING_TRANSFER);  //?  : TransferFlags.VOID_PENDING_TRANSFER 4 or 8

        var batchResults = client.createTransfers(batch);

        if (batchResults.getLength() > 0) {
            batchResults.next();
            throw new TransferException(batchResults.getResult());
        }
        return findTransfersByBatchIds(List.of(UInt128.asUUID(id))).values().stream().toList().get(0);
    }

    public List<Account> fetchAccountsByBatchFilter(BatchFilter dataFilter)  {

        var filter = new QueryFilter();
        if(dataFilter.accountNumber() != null) {
            filter.setUserData128(UInt128.asBytes(dataFilter.accountNumber()));
        }
        filter.setUserData64(dataFilter.userData64());
        filter.setUserData32(dataFilter.userData32());
        filter.setReversed(dataFilter.reversed());
        if(dataFilter.fromDate() != null) {
            filter.setTimestampMin(dataFilter.fromDate().getTime() * 1000000);
        }
        if(dataFilter.toDate() != null) {
            filter.setTimestampMax(dataFilter.toDate().getTime() * 1000000);
        }
        filter.setLimit(dataFilter.limit());

        var batch = client.queryAccounts(filter);
        var result = new ArrayList<Account>();
        while (batch.next()) {
            Account account = Account.builder()
                    .id(UInt128.asUUID(batch.getId()))
                    .accountNumber(UInt128.asBigInteger(batch.getUserData128()))
                    .code(batch.getCode())
                    .ledger(batch.getLedger())
                    .userData32(batch.getUserData32())
                    .userData64(batch.getUserData64())
                    .creditsPending(batch.getCreditsPending())
                    .debtsPending(batch.getDebitsPending())
                    .creditsPosted(batch.getCreditsPosted())
                    .debtsPosted(batch.getDebitsPosted())
                    .flags(batch.getFlags())
                    .timestamp(batch.getTimestamp()) //convertTigerBeetleTimestampToDateTime(batch.getTimestamp())
                    .build();

            result.add(account);
        }

        return result;
    }



    public List<Transfer> fetchTransactionByBatchFilter(BatchFilter batchFilter)  {

        var filter = new QueryFilter();
        if(batchFilter.accountNumber() != null) {
            filter.setUserData128(UInt128.asBytes(batchFilter.accountNumber()));
        }
        filter.setUserData64(batchFilter.userData64());
        filter.setUserData32(batchFilter.userData32());
        filter.setReversed(batchFilter.reversed());
        if(batchFilter.fromDate() != null) {
            filter.setTimestampMin(batchFilter.fromDate().getTime() * 1000000);
        }
        if(batchFilter.toDate() != null) {
            filter.setTimestampMax(batchFilter.toDate().getTime() * 1000000);
        }
        filter.setLimit(batchFilter.limit());

        var batch = client.queryTransfers(filter);
        var result = new ArrayList<Transfer>();
        while (batch.next()) {
            Transfer tr = Transfer.builder()
                    .id(UInt128.asUUID(batch.getId()))
                    .code(batch.getCode())
                    .amount(batch.getAmount())
                    .flags(batch.getFlags())
                    .ledger(batch.getLedger())
                    .creditAccountId(UInt128.asUUID(batch.getCreditAccountId()))
                    .debitAccountId(UInt128.asUUID(batch.getDebitAccountId()))
                    .userData128(UInt128.asUUID(batch.getUserData128()))
                    .userData64(batch.getUserData64())
                    .userData32(batch.getUserData32())
                    .timestamp(convertTigerBeetleTimestampToDateTime(batch.getTimestamp()))
                    .pendingId(UInt128.asUUID(batch.getPendingId()))
                    .build();

            result.add(tr);
        }

        return result;
    }

    public List<Transfer> listAccountTransfers(Filters customFilter)  {

        var filter = new AccountFilter();
        filter.setAccountId(UInt128.asBytes(customFilter.accountId()));
        filter.setCredits(customFilter.credits());
        filter.setDebits(customFilter.debits());
//        filter.setReversed(lastFirst);
        if(customFilter.fromDate() != null) {
            filter.setTimestampMin(customFilter.fromDate().getTime() * 1000000);
        }
        if(customFilter.toDate() != null) {
            filter.setTimestampMax(customFilter.toDate().getTime() * 1000000);
        }
        filter.setLimit(customFilter.limit());

        var batch = client.getAccountTransfers(filter);
        var result = new ArrayList<Transfer>();
        while (batch.next()) {
            Transfer tr = Transfer.builder()
                    .id(UInt128.asUUID(batch.getId()))
                    .code(batch.getCode())
                    .amount(batch.getAmount())
                    .flags(batch.getFlags())
                    .ledger(batch.getLedger())
                    .creditAccountId(UInt128.asUUID(batch.getCreditAccountId()))
                    .debitAccountId(UInt128.asUUID(batch.getDebitAccountId()))
                    .userData128(UInt128.asUUID(batch.getUserData128()))
                    .userData64(batch.getUserData64())
                    .userData32(batch.getUserData32())
                    .timestamp(convertTigerBeetleTimestampToDateTime(batch.getTimestamp()))
                    .pendingId(UInt128.asUUID(batch.getPendingId()))
                    .build();

            result.add(tr);
        }

        return result;
    }

    public List<Balance> listAccountBalances(Filters queryFilter)  {
        var filter = new AccountFilter();
        filter.setAccountId(UInt128.asBytes(queryFilter.accountId()));
        filter.setCredits(queryFilter.credits());
        filter.setDebits(queryFilter.debits());
        filter.setLimit(queryFilter.limit());
//        filter.setReversed(lastFirst);
        if(queryFilter.fromDate() != null) {
            filter.setTimestampMin(queryFilter.fromDate().getTime() * 1000000);
        }
        if(queryFilter.toDate() != null) {
            filter.setTimestampMax(queryFilter.toDate().getTime() * 1000000);
        }

        var batch = client.getAccountBalances(filter);
        var result = new ArrayList<Balance>();
        while (batch.next()) {
            result.add(
                    Balance.builder()
                            .accountId(queryFilter.accountId())
                            .debitsPending(batch.getDebitsPending())
                            .debitsPosted(batch.getDebitsPosted())
                            .creditsPending(batch.getCreditsPending())
                            .creditsPosted(batch.getCreditsPosted())
                            .timestamp(convertTigerBeetleTimestampToDateTime(batch.getTimestamp()))
                            .build()
            );
        }

        return result;
    }

    public static String convertTigerBeetleTimestampToDateTime(long tigerBeetleTimestampNanos) {
        // Convert nanoseconds to milliseconds
        long timestampMillis = tigerBeetleTimestampNanos / 1_000_000;

        // Create an Instant from the milliseconds
        Instant instant = Instant.ofEpochMilli(timestampMillis);

        // Convert the Instant to a ZonedDateTime in UTC
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("UTC"));

        // Format the ZonedDateTime with the pattern: yyyy-MM-dd HH:mm:ss.SSS z
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");

        return zonedDateTime.format(formatter);
    }
    public List<Map.Entry<UUID, CreateTransferResult>> createLinkedTransfers(List<Transfer> transfers)  {

        var results = new ArrayList<Map.Entry<UUID, CreateTransferResult>>(transfers.size());
        var batch = new TransferBatch(transfers.size());
        for (Transfer t : transfers) {
            byte[] id = UInt128.id();
            batch.add();
            batch.setId(id);

            // Is this the last transfer to add ?
            if (batch.getPosition() != transfers.size() - 1) {
                batch.setFlags(TransferFlags.LINKED);
            }

            batch.setLedger(t.ledger());
            batch.setAmount(t.amount());
            batch.setDebitAccountId(UInt128.asBytes(t.debitAccountId()));
            batch.setCreditAccountId(UInt128.asBytes(t.creditAccountId()));
            if (t.userData128() != null) {
                batch.setUserData128(UInt128.asBytes(t.userData128()));
            }
            batch.setCode(t.code());
            results.add(new AbstractMap.SimpleImmutableEntry<>(UInt128.asUUID(id), CreateTransferResult.Ok));
        }

        var batchResult = client.createTransfers(batch);
        while (batchResult.next()) {
            var original = results.get(batchResult.getIndex());
            results.set(batchResult.getIndex(), new AbstractMap.SimpleImmutableEntry<>(original.getKey(), batchResult.getResult()));
        }

        return results;
    }

}
