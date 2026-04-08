# jtl-to-html-parser
Small util to convert .jtl files to Html reports created as replacement for default jmeter util as its slow and resource hungry.

## Performance
- 500,000,000 rows (85.8GB): ~286 sec with 12cpu x 512mb batch size

## Usage 
```
jtlstats <path to .jtl> <delimiter> <output>
```
```
jtlstats ./result.jtl , report.html
```