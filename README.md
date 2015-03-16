# MongoTools
A simple set of tools written for administration of MongoDB servers. All the tools available via this project works via CLI and were tested on Mono aswell, so you can run them on your unix environment if you feel like

This set of tools takes advantage of Parallel processing to perform each operation in it's own thread. Since MongoDB uses, for some operations, a "Collection-Level" lock,
by using one thread per collection processing, I can take full advantage of both the database performance and the network speed.

Read the Wiki for the parameters and examples of each  tool.

Tools Available
======================
**Migrate**  : Migrates data (and indexes) from one database to another (or from a server to another).

**Duplicate** : Duplicates collections within the same database (copying indexes if necessary).