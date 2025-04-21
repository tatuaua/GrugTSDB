# GrugTSDB

## Done:
* Basic In-Memory Storage: Implemented a simple in-memory storage engine for time series data.
* Data Ingestion: Ability to ingest time series data points into the storage engine.
* Basic Querying: Implemented basic querying capabilities to retrieve time series data.
* Basic Streaming: Implemented a simple streaming mechanism to allow streaming writes to a UDP client.
* Timestamping: Ability to associate data points with specific points in time.
* Querying by Time Range: Efficient retrieval of data within specified time intervals.
* Basic Aggregations: Support for common aggregate functions (e.g., average, sum, min, max, count) over time windows.

## Roadmap:
* Data Retention Policies: Mechanisms to automatically manage data lifecycle, including deletion or archiving of older data.
* Data Tagging/Metadata: Ability to associate additional labels or metadata with time series data for filtering and organization.
* Persistence: Ability to restart engine without losing all data