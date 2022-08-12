
This code has two programs:

1) `bin/reader_xml`
2) `bin/reader_bin`

The first one reads a XML dump of Wikipedia articles and converts from XML
to a custom binary format. The binary format can be read *much* faster.
It is read by the second program.

The second program reads the binary files produced by the first one. This
file contains wikipedia articles. It will then mass insert those articles
in a Postgres database.

The insertion occurs in parallel, using the `-w` option (number of workers).
The goal of the project is to be able to benchmark different postgres configurations
with a fairly large dataset.

## Steps

Assuming the file `/tmp/enwiki-20220801-pages-articles.xml.bz2` exists:

1) Create the folder to output the binary files: `mkdir /tmp/pages`

2) Run the program to convert the XML file to the binary files:

```shell
bundle exec bin/reader_xml.rb \
  --input /tmp/enwiki-20220801-pages-articles.xml.bz2 \
  --out-dir /tmp/pages
```

**Warning**: output files will be overwritten.

This can take about 1 hour to finish. Once this is done, the XML file is not
necessary anymore.

3) Assuming a postgres database called `out_db` exists in localhost and has
user `foo` with password `bar` configured and the table `wiki_pages` (see schema below)
also exists:

```shell
bundle exec bin/reader_bin.rb \
  --path /tmp/pages \
  --db-url 'postgres://foo:bar@127.0.0.1/out_db' \
  --log /tmp/insertion_log.log \
  -w 16
```

This will do the mass insert into the postgres database.
**Note**: this assumes the table `wiki_pages` is empty.

The output in file `/tmp/insertion_log.log` can be used to analyse the run.

The parameter `-w` determines the amount of simultaneous inserts onto the database.
In the example, 16 workers will be used, which also means 16 connections will be established with the DB.

## Input file

The XML file to be used as the input can be [downloaded here](https://en.wikipedia.org/wiki/Wikipedia:Database_download#Where_do_I_get_it?),
or more specifically, [here](https://dumps.wikimedia.org/enwiki/latest/).
The file has the name like `enwiki-latest-pages-articles.xml.bz2`. It is NOT the multistream thing.
The `.xml.bz2` must NOT be decompressed before being fed to the `bin/reader_xml` program,
as it already expects the compressed file.

## DB schema

The `bin/reader_bin` program expects the database to have the following table only:

```sql
CREATE TABLE wiki_pages (
 page_id INTEGER PRIMARY KEY,
 revision_id INTEGER,
 parent_id INTEGER,
 ts TIMESTAMP WITHOUT TIME ZONE,
 sha1 TEXT,
 is_redirect BOOLEAN NOT NULL,
 revision_text TEXT,
 title TEXT
);
```
