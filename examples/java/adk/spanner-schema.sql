-- =============================================================================
-- 1. BASE RELATIONAL TABLES
-- =============================================================================

-- Accounts Node Table
CREATE TABLE Account (
    AccountId STRING(64) NOT NULL,
    AccountHolder STRING(256) NOT NULL,
    Status STRING(32) NOT NULL, -- 'PENDING', 'CLEARED', 'REVIEW_REQUIRED'
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (AccountId);

-- Shared Attributes Node Tables (For Synthetic/Co-location Checks)
CREATE TABLE SharedDevice (
    DeviceId STRING(128) NOT NULL,
    DeviceModel STRING(128),
    FirstSeen TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (DeviceId);

-- Edge Table: Linking Accounts to Devices
CREATE TABLE AccountDevice (
    AccountId STRING(64) NOT NULL,
    DeviceId STRING(128) NOT NULL,
    LinkedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    FOREIGN KEY (AccountId) REFERENCES Account (AccountId),
    FOREIGN KEY (DeviceId) REFERENCES SharedDevice (DeviceId)
) PRIMARY KEY (AccountId, DeviceId);

-- Edge Table: Financial Transactions (Edges between Accounts)
CREATE TABLE Transactions (
    TransactionId STRING(64) NOT NULL,
    SenderId STRING(64) NOT NULL,
    ReceiverId STRING(64) NOT NULL,
    Amount NUMERIC NOT NULL,
    Status STRING(32) NOT NULL, -- 'PENDING', 'CLEARED', 'REVIEW_REQUIRED'
    RiskReason STRING(MAX),
    Timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    ReviewedAt TIMESTAMP,
    FOREIGN KEY (SenderId) REFERENCES Account (AccountId),
    FOREIGN KEY (ReceiverId) REFERENCES Account (AccountId)
) PRIMARY KEY (TransactionId);

-- Change Stream for Transactions
CREATE CHANGE STREAM TransactionsStream FOR Transactions
  OPTIONS (
    exclude_update = true,
    exclude_delete = true
  );

-- =============================================================================
-- 2. SPANNER PROPERTY GRAPH DEFINITION
-- =============================================================================

CREATE PROPERTY GRAPH FinancialGraph
  NODE TABLES (
    Account,
    SharedDevice
  )
  EDGE TABLES (
    Transactions
      SOURCE KEY (SenderId) REFERENCES Account (AccountId)
      DESTINATION KEY (ReceiverId) REFERENCES Account (AccountId)
      LABEL TRANSFERRED_TO,
    AccountDevice
      SOURCE KEY (AccountId) REFERENCES Account (AccountId)
      DESTINATION KEY (DeviceId) REFERENCES SharedDevice (DeviceId)
      LABEL USED_DEVICE
  );
