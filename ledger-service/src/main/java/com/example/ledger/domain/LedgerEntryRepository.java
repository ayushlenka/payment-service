package com.example.ledger.domain;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

    boolean existsByTransferId(UUID transferId);
}
