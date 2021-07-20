# Testing performance with PostgreSQL and Citus in Data Vault tables

We have three common data vault tables: a Hub, a Satellite and a Reference table.
In this experiment, we want to find out the performance of COPY-ing data into the tables and for retrieving the data again.
We apply distribution using Citus and create indexes, with the goal to see how these impact that performance.
This is an initial exploration of how to build a Data Vault model in Azure PostgreSQL Hyperscale.

We always have 10000 Hub inserts and then the Sat and Reference table inserts are a multiple of that.

```
bash experiment.sh <hostname> <dbname> <user>
```
