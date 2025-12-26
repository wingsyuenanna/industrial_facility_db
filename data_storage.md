             +------------------------+
             |  Local ETL / DuckDB   |
             |  CSV -> Parquet        |
             +-----------+------------+
                         |
                         | Upload
                         v
          +-------------------------------+
          |        Amazon S3               |
          | (Raw / Bronze / Silver / Gold)|
          +-------------------------------+
            |           |             |
            |           |             |
  Partitions: sector, emission_type, year
  Files: Parquet, compressed (ZSTD/Snappy)
            |
            v
  +----------------------------+
  |  AWS Glue Data Catalog      |
  |  - Table metadata           |
  |  - Column types             |
  |  - Partitions               |
  +----------------------------+
            |
            v
  +----------------------------+
  |   Amazon Athena            |
  |  - Serverless SQL queries  |
  |  - Reads only needed cols  |
  |  - Uses partition pruning  |
  +----------------------------+
