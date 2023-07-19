package org.apache.bookkeeper.utils;

public class Utility {

    public enum MasterKeyStatus {
        VALID_KEY,
        NULL_KEY
    }

    public enum EntryStatus {
        VALID_ENTRY,
        NOT_VALID_ENTRY,
        NULL_ENTRY
    }

    public enum ReadEntryStatus {
        EXISTING_ENTRY,
        NOT_EXISTING_ENTRY
    }

    public enum EntryListStatus {
        ALL_VALID,
        ONE_NOT_VALID,
        ONE_NULL
    }

    public enum LedgerStatus {
        EXISTING_LEDGER,
        NOT_EXISTING_LEDGER
    }
}
