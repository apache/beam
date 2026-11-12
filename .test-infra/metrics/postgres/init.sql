--  Licensed to the Apache Software Foundation (ASF) under one
--  or more contributor license agreements.  See the NOTICE file
--  distributed with this work for additional information
--  regarding copyright ownership.  The ASF licenses this file
--  to you under the Apache License, Version 2.0 (the
--  "License"); you may not use this file except in compliance
--  with the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
-- limitations under the License.

CREATE extension tablefunc;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'kubeproxyuser_ro') THEN
    CREATE ROLE kubeproxyuser_ro NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION;
  END IF;
END
$$;

ALTER ROLE kubeproxyuser_ro NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION;
ALTER ROLE kubeproxyuser_ro SET default_transaction_read_only = on;
REVOKE ALL ON SCHEMA public FROM kubeproxyuser_ro;
GRANT USAGE ON SCHEMA public TO kubeproxyuser_ro;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC, kubeproxyuser_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO kubeproxyuser_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM PUBLIC, kubeproxyuser_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO kubeproxyuser_ro;
