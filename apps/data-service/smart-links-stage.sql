PRAGMA defer_foreign_keys=TRUE;
CREATE TABLE links (
	link_id text PRIMARY KEY NOT NULL,
	account_id text NOT NULL,
	destinations TEXT NOT NULL,
	created numeric DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
	updated numeric DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
	name text NOT NULL
);
INSERT INTO "links" VALUES('_yCI8T2Ah5','1234567890','{"default":"https://alpinejs.dev/"}','2025-10-05 21:39:08','2025-10-05 21:39:08','Alpine JS');
INSERT INTO "links" VALUES('rf0iZ16A0f','1234567890','{"default":"https://kaplayjs.com/"}','2025-10-05 21:45:58','2025-10-05 21:45:58','Kaplay JS');
INSERT INTO "links" VALUES('zgkXrMm7z8','1234567890','{"default":"https://alpine-ajax.js.org/"}','2025-10-06 02:05:21','2025-10-06T22:28:12.408Z','Alpine AJAX');
INSERT INTO "links" VALUES('11W5RHcL3k','1234567890','{"default":"https://crosscolor.dehkr25.workers.dev/"}','2025-10-06 22:11:36','2025-10-06T22:17:33.515Z','Crosscolor');
INSERT INTO "links" VALUES('Hebyj5Ib14','1234567890','{"default":"https://www.jetpens.com/"}','2025-10-12 17:41:50','2025-10-12 17:41:50','JetPens');
INSERT INTO "links" VALUES('m-9vdJUhX4','1234567890','{"default":"https://developer.mozilla.org/en-US/"}','2025-10-14 03:21:37','2025-10-14 03:21:37','MDN Web Docs');
