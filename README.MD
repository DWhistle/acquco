## Design considerations


1. We could use the given builder for making simple queries.
2. Later on we could evolve it to a builder, which would be able to work with joints.
3. To make selects faster, we add B-Tree (preferred) or HASH-based indexes for the account_id and ad_group_id. Indexes could also be added to date-related fields if we consider filtering by dates.
4. Concurrency in querying could be reached by sharding and doing map-reduce operations on shards. There is no need to do it explicitly on a single instance as it is natively handled by the database engine in most cases
5. Queries like "Facebook Ad Orders Per Click" can be built upon 2 simple queries.
6. Queries like "Facebook Ad Event" are better built with no explicit abstractions initially as the abstractions could be too complicated. Should be abstracted in case of absolute necessity
7. More with_*condition* could be added to enhance the query, like grouping