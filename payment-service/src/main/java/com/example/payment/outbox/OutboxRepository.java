package com.example.payment.outbox;

import java.util.List;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {

    // Oldest first, so events keep their production order when relayed.
    List<OutboxMessage> findByPublishedAtIsNullOrderByIdAsc(Limit limit);
}
