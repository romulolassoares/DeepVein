# Project Charlie

**Project Charlie** is a backend system for managing, organizing, and executing SQL queries across multiple database engines, extended with custom Python logic registered directly into [DuckDB](https://duckdb.org/).

This repository (**DeepVein**) is where the implementation lives. The project is in early development; APIs and layout will evolve as the roadmap below is implemented.

## Overview

Charlie provides a single conceptual layer over heterogeneous databases: define connections once, register named queries and groups, run them in bulk or one at a time, and optionally enrich DuckDB with Python-defined functions (UDFs) for analytics and glue logic.

## Core features

### Multi-database connectivity

Connections to common engines—including **SQL Server**, **PostgreSQL**, and others—through one interface for registering and using active connections.

### Query registry

A structured way to:

- Register and persist **named** SQL queries  
- Assign queries to **groups** for organization  
- Run queries **individually** or trigger a **whole group** in one go  

### Custom Python functions in DuckDB

Define, register, and manage Python functions inside DuckDB so SQL can call application-specific logic beyond built-ins.

## Roadmap

### Phase 1 — Database connectivity

- [ ] Define the base connection interface  
- [ ] Implement SQL Server connection adapter  
- [ ] Implement PostgreSQL connection adapter  
- [ ] Implement DuckDB connection adapter  
- [ ] Connection manager to register and retrieve active connections  

### Phase 2 — Query execution

- [ ] `execute_query` — return full result set  
- [ ] `execute_query_stream` — yield results in chunks for large datasets  
- [ ] Error handling and query timeouts  

### Phase 3 — Data extraction

- [ ] `extract_to_parquet` — run a query and write results to Parquet  
- [ ] `extract_to_parquet_stream` — stream rows into Parquet to bound memory  

### Phase 4 — Query registry

- [ ] Choose and document storage format (e.g. YAML, JSON, TOML, or table-backed) for named queries and group membership  
- [ ] `QueryRegistry` — load, validate, and persist query definitions  
- [ ] `QueryGroup` — named sets of queries with ordered or parallel execution  
- [ ] `QueryRunner` (or equivalent) — accept groups or named queries, resolve connections, dispatch standard vs streaming execution, aggregate or route results  

### Phase 5 — DuckDB Python UDFs

- [ ] Interface for registering custom Python functions in DuckDB  
- [ ] `UDFRegistry` — store, validate, and version UDF definitions  
- [ ] Auto-register UDFs when DuckDB connections start  
- [ ] Reload / hot-swap UDFs without a full restart  

### Phase 6 — Hardening and developer experience

- [ ] Structured logging across components  
- [ ] Unit tests for adapters, execution, and registry behavior  
- [ ] Type hints and docstrings throughout  
- [ ] Usage guide and example configurations  

## Contributing

Contributions are welcome. Because the codebase is still taking shape, the smoothest path is:

1. **Check the roadmap** — pick something that matches your goals, or propose an addition if the work does not fit an existing phase.
2. **Open an issue** (or comment on one) for larger design or API changes so direction stays aligned before you invest significant time.
3. **Fork and branch** — use a focused branch and keep pull requests scoped to one concern when possible.
4. **Pull request** — describe what changed and why; link related issues when applicable.

Patches must be compatible with this project’s **GPL-3.0** license. By contributing, you agree your contributions will be licensed under the same terms.

## License

This project is licensed under the **GNU General Public License v3.0** — see [LICENSE](LICENSE).
