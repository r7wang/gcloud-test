# Google Cloud

## Build
`make` will build each of the four relevant binaries.

## Performance
The table below lists latency across a variety of operation types in `milliseconds`. The values were calculated across `1000` samples, with the first `10` samples discarded for performance consistency. Not all operation types are natively supported by the database technology, however Spanner can imitate any operation type through more complex constructs.

| Operation           | Bigtable Average (99pct) | Spanner Average (99pct)
| :-----------------: | :----------------------: | :---------------------:
| simpleRandomRead    | 32.05 (88.54)            | TBD
| multiRandomRead     | 63.36 (88.41)            | TBD
| multiSequentialRead | 34.46 (45.91)            | TBD
| blindWrite          | 29.96 (33.33)            | TBD
| delete              | 29.98 (33.65)            | TBD
| atomicAppend        | 45.09 (49.69)            | N/A
