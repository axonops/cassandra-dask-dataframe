# Distributed Architecture

## How async-cassandra-dataframe Works with Dask Cluster

```
┌─────────────────┐
│   Client Code   │
│  (Your Script)  │
└────────┬────────┘
         │
         │ 1. df = await cdf.read_cassandra_table(...)
         │
┌────────▼────────┐
│ async-cassandra │
│   -dataframe    │
└────────┬────────┘
         │
         │ 2. Discovers token ranges from Cassandra
         │    Creates partition tasks
         │
┌────────▼────────┐
│ Dask Scheduler  │
│ (Coordinator)   │
└────────┬────────┘
         │
         │ 3. Distributes tasks to workers
         │
    ┌────┴────┬────────┬────────┐
    │         │        │        │
┌───▼───┐ ┌──▼───┐ ┌──▼───┐ ┌──▼───┐
│Worker1│ │Worker2│ │Worker3│ │  ...  │
└───┬───┘ └──┬───┘ └──┬───┘ └──┬───┘
    │        │        │        │
    │ 4. Each worker:           │
    │    - Receives partition info
    │    - Creates Cassandra connection
    │    - Executes query for its token range
    │    - Returns DataFrame partition
    │        │        │        │
┌───▼────────▼────────▼────────▼───┐
│       Cassandra Cluster           │
│  ┌──────┐ ┌──────┐ ┌──────┐     │
│  │Node 1│ │Node 2│ │Node 3│     │
│  └──────┘ └──────┘ └──────┘     │
└───────────────────────────────────┘
```

## Key Points:

1. **Connection Per Worker**: Each Dask worker creates its own connection to Cassandra
2. **Token-Aware Routing**: Workers query specific token ranges, leveraging Cassandra's distributed nature
3. **Parallel Execution**: Multiple workers query different parts of the data simultaneously
4. **No Data Serialization**: Workers read directly from Cassandra, no data passes through scheduler

## Example Flow:

1. Your code: `df = await cdf.read_cassandra_table("my_table", session=session)`
2. Library discovers Cassandra has 256 token ranges across 3 nodes
3. Library creates 20 Dask partitions (grouped token ranges)
4. Dask scheduler assigns partitions to 3 workers
5. Each worker:
   - Gets partition metadata (table, keyspace, token range)
   - Creates connection to Cassandra using same connection params
   - Executes: `SELECT * FROM my_table WHERE token(pk) > X AND token(pk) <= Y`
   - Returns pandas DataFrame for that partition
6. Dask combines all partitions into final DataFrame

## Why This Works:

- **Cassandra Connection Info**: Passed as serializable parameters (host, port, keyspace)
- **Token Ranges**: Simple integers that define query boundaries
- **Stateless Queries**: Each query is independent, no shared state
- **Cassandra's Design**: Built for parallel token-range queries