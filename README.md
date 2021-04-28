# Parquet 

Work in progress.

# Motivation

The existing go parquet implementation suffer from excessive memory usage, or poor performance.
For our use case go struct to parquet conversion has to come with both small memory footprint,
good compression and fast performance. In addition, conversion has to support omit empty style setting, where empty
string, zero or false shall not produce any value. This is especially important when ingesting data to BigQuery.

This library has been forked from [Parsyl](https://github.com/parsyl/parquet) and modified to meet our goals.


# Usage

