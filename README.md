
```sql

CREATE TABLE wiki_pages (
 page_id INTEGER PRIMARY KEY,
 revision_id INTEGER,
 parent_id INTEGER,
 ts TIMESTAMP WITHOUT TIME ZONE,
 sha1 TEXT,
 is_redirect BOOLEAN NOT NULL,
 revision_text TEXT
);

```
