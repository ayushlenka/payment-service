package com.example.ledger.posting;

import java.util.UUID;

/** Outcome of posting a transfer. */
public record PostingResult(Outcome outcome, UUID transferId, String reason) {

    public enum Outcome { POSTED, REJECTED, ALREADY_POSTED }

    public static PostingResult posted(UUID transferId) {
        return new PostingResult(Outcome.POSTED, transferId, null);
    }

    public static PostingResult rejected(UUID transferId, String reason) {
        return new PostingResult(Outcome.REJECTED, transferId, reason);
    }

    public static PostingResult alreadyPosted(UUID transferId) {
        return new PostingResult(Outcome.ALREADY_POSTED, transferId, null);
    }
}
