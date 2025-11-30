PRAGMA defer_foreign_keys=TRUE;
CREATE TABLE links (
	link_id text PRIMARY KEY NOT NULL,
	account_id text NOT NULL,
	destinations TEXT NOT NULL,
	created numeric DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
	updated numeric DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
	name text NOT NULL
);
