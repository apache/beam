-- Seed Accounts
INSERT INTO Account (AccountId, AccountHolder, Status, CreatedAt) VALUES
  ('usr_alice', 'Alice Smith', 'CLEARED', CURRENT_TIMESTAMP()),
  ('usr_bob', 'Bob Jones', 'CLEARED', CURRENT_TIMESTAMP()),
  ('usr_charlie', 'Charlie Brown', 'CLEARED', CURRENT_TIMESTAMP()),
  ('usr_mule1', 'Mule One', 'CLEARED', CURRENT_TIMESTAMP()),
  ('usr_mule2', 'Mule Two', 'CLEARED', CURRENT_TIMESTAMP()),
  ('usr_collector', 'Collector Hub', 'CLEARED', CURRENT_TIMESTAMP()),
  ('usr_fraudA', 'Fraud User A', 'CLEARED', CURRENT_TIMESTAMP()),
  ('usr_fraudB', 'Fraud User B', 'CLEARED', CURRENT_TIMESTAMP());

-- Seed Shared Device
INSERT INTO SharedDevice (DeviceId, DeviceModel, FirstSeen) VALUES
  ('dev_hardware_xyz99', 'iPhone 15 Pro', CURRENT_TIMESTAMP());

-- Scenario 1: Circular Flow Setup
INSERT INTO Transactions (TransactionId, SenderId, ReceiverId, Amount, Status, Timestamp) VALUES
  ('tx_loop_01', 'usr_alice', 'usr_bob', 10000.00, 'CLEARED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 HOUR)),
  ('tx_loop_02', 'usr_bob', 'usr_charlie', 9800.00, 'CLEARED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 HOUR));

-- Scenario 2: Fan-In Structuring Setup
INSERT INTO Transactions (TransactionId, SenderId, ReceiverId, Amount, Status, Timestamp) VALUES
  ('tx_fan_01', 'usr_mule1', 'usr_collector', 9200.00, 'CLEARED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)),
  ('tx_fan_02', 'usr_mule2', 'usr_collector', 9400.00, 'CLEARED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY));

-- Scenario 3: Shared Device Setup
INSERT INTO AccountDevice (AccountId, DeviceId, LinkedAt) VALUES
  ('usr_fraudA', 'dev_hardware_xyz99', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)),
  ('usr_fraudB', 'dev_hardware_xyz99', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY));
