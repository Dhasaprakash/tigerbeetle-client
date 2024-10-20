package com.tigerbeetle.client.controller;

import com.tigerbeetle.client.dto.*;
import com.tigerbeetle.client.repository.AccountRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping(path = "/v1/tiger-beetle")
public class TigerBeetleClientController {

    @Autowired
    AccountRepository repo;

    @PostMapping("/accounts")
    public List<Account> processRequest(@RequestBody List<Account> accounts) {
        return repo.createAccountBatch(accounts);
    }

    @GetMapping("/accounts/{id}")
    public Account fetchAccounts(@PathVariable("id") UUID id) {
        return repo.findAccountById(id).orElseThrow(() -> new RuntimeException("Not found"));
    }

    @PostMapping("accounts/lookup")
    public List<Account> lookupListOfAccounts(@RequestBody UUID[] ids) {
        return repo.findAccountsById(ids).values().stream().toList();
    }

    @PostMapping("/batch/transfers")
    public List postBatchTransfers(@RequestBody List<Transfer> transfers) {
        return repo.createBatchTransfer(transfers);
    }

    @PostMapping("/pending/transfers")
    public Transfer postPendingTransfers(@RequestBody Transfer transfers) {
        return repo.createPendingTransfer(transfers);
    }

    @PutMapping("/pending/transfers")
    public Transfer resolveTransfers(@RequestBody Transfer transfers) {
        return repo.completePendingTransfer(transfers);
    }

    @PostMapping("/transactions/history")
    public List<Transfer> fetchTransactionsByCriteria(@RequestBody Filters filter) {
        return repo.listAccountTransfers(filter);
    }

    @PostMapping("/accounts/extraction")
    public List<Account> fetchAccountsByBatchFilter(@RequestBody BatchFilter filter) {
        return repo.fetchAccountsByBatchFilter(filter);
    }

    @PostMapping("/transactions/extraction")
    public List<Transfer> fetchTransactionsByBatchFilter(@RequestBody BatchFilter filter) {
        return repo.fetchTransactionByBatchFilter(filter);
    }

    @PostMapping("/balance/history")
    public List<Balance> fetchBalanceByCriteria(@RequestBody Filters filter) {
        return repo.listAccountBalances(filter);
    }

    @GetMapping("/transfers/{id}")
    public Transfer fetchTxnById(@PathVariable("id") UUID id) {
        return repo.findTransfersById(id);
    }

}
