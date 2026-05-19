# Wikipedia benchmark data

The Wikipedia benchmark requires a single data file at:

```
benchmark/src/wikipedia/articles
```

This file is **not** stored in the repository (it is ~1.4 GB). It must be
downloaded or generated separately before building/running the
`LeanStore_Wiki`, `Filesystem_Wiki`, or `LeanStore_IndexWiki` benchmarks.

## Format

The file contains one JSON object per line, each representing a non-empty
article from an English Wikipedia (`enwiki`) dump. See
`benchmark/src/include/benchmark/wikipedia/workload.h` for how it is consumed.

The companion file `summary.csv` (committed alongside this README) holds the
per-article `page_len` / `monthly_views` characteristics and is read via the
`--wiki_workload_config_path` flag.

## Obtaining the file

Any of the following will work:

1. **Generate from an enwiki dump.** Download a recent
   `enwiki-*-pages-articles.xml.bz2` from
   `https://dumps.wikimedia.org/enwiki/`, then extract one JSON object per
   non-empty article into `articles`, in the same order used by `summary.csv`.

2. **Copy from a colleague / lab share.** If you have access to the original
   data set used in our experiments, place it at the path above.

## Pointing the benchmark at a different location

By default the benchmark uses the compile-time path baked into
`benchmark/src/wikipedia/config.cc` (generated from `config.cc.in`). To use a
file in another directory without rebuilding, pass:

```
--wiki_articles_path=/absolute/path/to/articles
```

at runtime.
