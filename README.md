# jtl-to-html-parser
Small util to convert .jtl files to html reports created as replacement for default jmeter util as its slow and resource hungry.

on my weak laptop it can parse 9gb (~56000000 rows) in around 10mins

usage 
```
jtlstats <path to .jtl> <delimiter> <output>
```
```
jtlstats ./result.jtl , report.html
```