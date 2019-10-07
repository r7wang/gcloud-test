# Google Cloud

## Build
`make` will build each of the four relevant binaries.

## Performance
The table below lists latency across a variety of operation types in `milliseconds`. The values were calculated across `1000` samples, with the first `10` samples discarded for performance consistency. Not all operation types are natively supported by the database technology, however Spanner can imitate any operation type through more complex constructs.

| Operation           | Bigtable Average (99pct) | Spanner Average (99pct)
| :-----------------: | :----------------------: | :---------------------:
| simpleRandomRead    | 32.05 (88.54)            | 33.50 (43.61)
| simpleRandomQuery   | N/A                      | 32.59 (38.12)
| multiRandomRead     | 63.36 (88.41)            | 175.68 (196.04)
| multiSequentialRead | 34.46 (45.91)            | 33.15 (38.84)
| blindWrite          | 29.96 (33.33)            | 48.00 (54.56)
| delete              | 29.98 (33.65)            | 48.33 (59.38)
| atomicAppend        | 45.09 (49.69)            | N/A
| atomicSwap          | N/A                      | 94.59 (104.25)
