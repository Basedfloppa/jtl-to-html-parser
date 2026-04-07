# jtl-to-html-parser
Small util to convert .jtl files to Html reports created as replacement for default jmeter util as its slow and resource hungry.

## Performance
- 100,000 rows: ~0.04 seconds
- 1,000,000 rows: ~0.14 seconds  
- 56,000,000 rows (9GB): ~10 minutes (as reported on older hardware)

The implementation uses parallel CSV parsing with memory-mapped files and Rayon for data parallelism, providing significant speed improvements over sequential parsing.

## Usage 
```
jtlstats <path to .jtl> <delimiter> <output>
```
```
jtlstats ./result.jtl , report.html
```