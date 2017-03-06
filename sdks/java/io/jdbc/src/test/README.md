These are instructions for maintaining postgres as needed for Integration Tests (JdbcIOIT).

You can always ignore these instructions if you have your own postgres cluster to test against.

Setting up Postgres
-------------------
1. Setup kubectl so it is configured to work with your kubernetes cluster
1. Run the postgres setup script
    src/test/resources/kubernetes/setup.sh
1. Do the data loading - create the data store instance by following the instructions in JdbcTestDataSet

... and your postgres instances are set up!

