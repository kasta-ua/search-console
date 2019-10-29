Script to download Google Search Console data. Requires [boot][] to run.

[boot]: https://boot-clj.com/

Written in such a way to utilize 20 QPS limit of Search Console API to maximum.

Usage:

```
boot search_console.boot -t path-to-google-token.json -d 2019-09-26
```
