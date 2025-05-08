##
## converts a 4plebs csv data dump to sqlite
## doesn't follow any specific schema in particular, just something for my own use
## the clevercsv library was needed for correct parsing (something to do with empty fields and quotes?)
## some stuff is hardcoded to assume /f/ and flashes but it should be adaptable for other boards too
##
import os
import sys
import math
import time
import base64
import sqlite3
import datetime
import zoneinfo
import clevercsv # type: ignore
import html.parser
from typing import Any, Dict, Final, Iterator, Optional

# note: reply thumbs smaller, don't use

# note: the CSV has posts in chronological order: oldest post first

# note: thumb_filename is always set. in some cases, the actual file might be missing from the thumbnail dump

# filenames seem to be html-escaped in the csv. source:
# sqlite3 f_filenames.db 'SELECT filename FROM f_reposts;' | grep -P '&(?!#)(?!amp;|quot;|lt;|gt;)'
# ^ the only occurrences of '&' are in html entities
# sqlite3 f_filenames.db 'SELECT filename FROM f_reposts;' | grep -P '["<>]'
# ^ the (non-numeric-escape) characters that are escaped using entities don't appear by themselves
# sqlite3 f_filenames.db 'SELECT filename FROM f_reposts;' | grep -P '&#(?!039;)'
# ^ the only numeric escape used is for apostrophe
# sqlite3 f_filenames.db 'SELECT filename FROM f_reposts;' | grep -P "'"
# ^ apostrophe can appear un-quoted. checked and both forms appear as ' on 4plebs
#   this is unambiguous so it should be harmless to un-escape the escaped ones
# NOTE: run the commands on a db made with html.parser.unescape() commented out!

# note: in f_reposts, replies are only counted for threads, not replies that contain a flash
# replies with files were only possible through the 4chan API (or something) so they should be rare

# note: yield type is really Dict[str, Optional[str]] but that'd require casts at the call site, not worth it
def read_dump_csv(f_csv: str) -> Iterator[Dict[str, Any]]:
	fieldnames: Final = (
		'num',
		'subnum',
		'thread_num',
		'op',
		'timestamp',
		'timestamp_expired',
		'preview_orig', # timestamp filename
		'preview_w',
		'preview_h',
		'media_filename', # user-provided filename
		'media_w',
		'media_h',
		'media_size', # bytes
		'media_hash', # md5 as url-safe packed base64
		'media_orig', # timestamp filename
		'spoiler',
		'deleted',
		'capcode',
		'email',
		'name',
		'trip',
		'title',
		'comment',
		'sticky',
		'locked',
		'poster_hash',
		'poster_country',
		'exif',
	)
	def fix_escapes(s: str) -> str:
		if '\\' in s:
			return s.replace('\\\n', '\n').replace('\\\\', '\\')
		else:
			return s
	with open(f_csv, mode='r', buffering=128*1024) as f:
		parser = clevercsv.cparser.Parser(
			f,
			delimiter=',',
			quotechar='"',
			escapechar='\\',
			field_limit=2000*4, # comment length limit TIMES the longest utf-8 byte sequence
			strict=False,
			return_quoted=True,
		)
		for row in parser:
			t = dict.fromkeys(fieldnames)
			for i, (field, was_quoted) in enumerate(row):
				if was_quoted:
					k = fieldnames[i]
					v = fix_escapes(field)
					t[k] = v
			yield t

fourchan_time: Final = zoneinfo.ZoneInfo('US/Eastern')
utc_time: Final      = zoneinfo.ZoneInfo('UTC')

def timestamp_to_isoformat(timestamp: int) -> str:
	dt = datetime.datetime.utcfromtimestamp(timestamp)
	dt = dt.replace(tzinfo=fourchan_time)
	dt = dt.astimezone(utc_time)
	stamp = dt.isoformat(sep=' ')
	stamp = stamp[:-6]
	return stamp

# remaining BUG: timestamps during the hour that's repeated when DST ends can't be un-fucked
# in the data, it looks like the time jumps backwards by an hour
# UTC is meant to be unaffected by DST but it's annoyingly tied to it with this
assert timestamp_to_isoformat(1641365611) == '2022-01-05 11:53:31' # http://127.1.1.1/dbtest.php?do=md5info&md5=628F5B4C9A2F074D3D11DFAC48D1B43C
assert timestamp_to_isoformat(1623750521) == '2021-06-15 13:48:41' # http://127.1.1.1/dbtest.php?do=md5info&md5=AAA92A58BFF44D5A56F3368C5F4E7170
assert timestamp_to_isoformat(1394905273) == '2014-03-15 21:41:13' # summer time - corresponds with file timestamp
assert timestamp_to_isoformat(1420053416) == '2015-01-01 00:16:56' # winter time - corresponds with file timestamp

def unescape_filename(filename: str) -> str:
	if '&' in filename:
		return html.unescape(filename)
	else:
		return filename

assert unescape_filename('&gt;implying.swf') == '>implying.swf'
assert unescape_filename('  < b >  hi  < / b > ') == '  < b >  hi  < / b > '
assert unescape_filename('  < b >  hi&amp;  < / b > ') == '  < b >  hi&  < / b > '
assert unescape_filename('  < b >  hi&amp;&amp;  < / b > ') == '  < b >  hi&&  < / b > '

exif_to_tag: Final = {
	'{"Tag":"Anime"}':    1,
	'{"Tag":"Game"}':     2,
	'{"Tag":"Hentai"}':   3,
	'{"Tag":"Japanese"}': 4,
	'{"Tag":"Loop"}':     5,
	'{"Tag":"Other"}':    6,
	'{"Tag":"Porn"}':     7,
}

def thumb_file_exists(name: str, thumbs_root: str) -> bool:
	path = f'{thumbs_root}/{name[0:4]}/{name[4:6]}/{name}'
	return os.access(path, os.F_OK)

def create(f_csv: str, db_path: str, thumbs_root: Optional[str]) -> None:

	db = sqlite3.connect(db_path, isolation_level=None)
	cursor = db.cursor()
	cursor.execute('PRAGMA journal_mode=WAL')
	cursor.execute('BEGIN DEFERRED TRANSACTION')

	# row for each instance of the flash being posted
	cursor.execute('''
	DROP TABLE IF EXISTS f_reposts
	''')
	cursor.execute('''
	CREATE TABLE f_reposts (
		threadnum INTEGER NOT NULL CHECK(threadnum>=1 AND threadnum<=postnum),
		postnum   INTEGER NOT NULL CHECK(postnum>=1 AND postnum>=threadnum),
		md5       BLOB    NOT NULL CHECK(LENGTH(md5)=16),
		filename  TEXT    NOT NULL CHECK(SUBSTR(filename, -4)='.swf'),
		timestamp TEXT    NOT NULL CHECK(LENGTH(timestamp)=19),
		replies   INTEGER NOT NULL CHECK(replies>=0) DEFAULT 0,
		tag       INTEGER NOT NULL CHECK(tag>=0 AND tag<=7)
	) STRICT
	''')

	# row for each unique flash with stuff that's the same each time it's posted
	cursor.execute('''
	DROP TABLE IF EXISTS f_reposts_meta
	''')
	cursor.execute('''
	CREATE TABLE f_reposts_meta (
		md5            BLOB    PRIMARY KEY NOT NULL CHECK(LENGTH(md5)=16),
		filesize       INTEGER             NOT NULL CHECK(filesize>=0),
		reposts        INTEGER             NOT NULL CHECK(reposts>=1),
		first_seen     TEXT                NOT NULL CHECK(first_seen<>'' AND first_seen<=last_seen),
		last_seen      TEXT                NOT NULL CHECK(last_seen<>'' AND last_seen>=first_seen),
		width_px       INTEGER             NOT NULL CHECK(width_px>=0),
		height_px      INTEGER             NOT NULL CHECK(height_px>=0),
		thumb_filename TEXT                         CHECK(LENGTH(thumb_filename)>=18 AND LENGTH(thumb_filename)<=21), -- e.g. "1396815618321s.jpg" to "1678066538836532s.jpg"
		data_filename  TEXT                         CHECK(LENGTH(data_filename)>=17) -- e.g. "1396815618321.swf"
	) STRICT
	''')

	cursor.execute('''
	DROP TABLE IF EXISTS f_comments
	''')
	cursor.execute('''
	CREATE TABLE f_comments (
		threadnum INTEGER NOT NULL CHECK(threadnum>=1 AND threadnum<=postnum),
		postnum   INTEGER NOT NULL CHECK(postnum>=1 AND postnum>=threadnum),
		subnum    INTEGER NOT NULL CHECK(subnum>=0),
		timestamp TEXT    NOT NULL CHECK(LENGTH(timestamp)=19),
		deleted   TEXT             CHECK(LENGTH(deleted)=19),
		name      TEXT             CHECK(name<>'' OR tripcode<>''),
		tripcode  TEXT             CHECK(tripcode<>''),
		email     TEXT             CHECK(email<>''),
		subject   TEXT             CHECK(subject<>''),
		comment   TEXT             CHECK(comment<>'')
	) STRICT
	''')
	# note: "deleted>=timestamp" is violated by 356 posts (2014-2015 only, might've been an archiver bug)

	print('read csv')

	rows = 0

	for row in read_dump_csv(f_csv):

		rows += 1

		postnum      = int(row['num'])
		subnum       = int(row['subnum'])
		threadnum    = int(row['thread_num'])
		timestamp    = timestamp_to_isoformat(int(row['timestamp']))
		deleted      = timestamp_to_isoformat(int(row['timestamp_expired'])) if row['timestamp_expired'] != '0' else None
		thumb        = row['preview_orig'] if thumbs_root is not None and row['preview_orig'] is not None and thumb_file_exists(row['preview_orig'], thumbs_root) else None
		filename     = unescape_filename(row['media_filename']) if row['media_filename'] not in (None, '') else None
		width        = int(row['media_w']) if row['media_w'] != '0' else 0
		height       = int(row['media_h']) if row['media_h'] != '0' else 0
		filesize     = int(row['media_size']) if row['media_size'] != '0' else 0
		md5          = base64.urlsafe_b64decode(row['media_hash']) if row['media_hash'] is not None else None
		datafilename = row['media_orig']
		email        = row['email']
		name         = row['name']
		tripcode     = row['trip']
		subject      = row['title']
		comment      = row['comment']
		tag          = exif_to_tag[row['exif']] if row['exif'] is not None else 0

		if md5 is not None:

			cursor.execute('''
			INSERT INTO f_reposts (threadnum, postnum, md5, filename, timestamp, tag)
			VALUES (?, ?, ?, ?, ?, ?)
			''', (threadnum, postnum, md5, filename, timestamp, tag))

			cursor.execute('''
			INSERT INTO f_reposts_meta (md5, filesize, reposts, first_seen, last_seen, width_px, height_px, thumb_filename, data_filename)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(md5) DO UPDATE
				SET reposts=reposts+1, last_seen=?, thumb_filename=IFNULL(thumb_filename, ?)
			''', (
				md5, filesize, 1, timestamp, timestamp, width, height, thumb, datafilename,
				timestamp, thumb))

		cursor.execute('''
		INSERT INTO f_comments (threadnum, postnum, subnum, timestamp, deleted, name, tripcode, email, subject, comment)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		''', (threadnum, postnum, subnum, timestamp, deleted, name, tripcode, email, subject, comment))

	print(f'total {rows} rows')

	print('set up f_reposts')
	cursor.execute('''
	CREATE INDEX f_reposts_md5 ON f_reposts(md5)
	''')
	cursor.execute('''
	CREATE INDEX f_reposts_filename ON f_reposts(filename)
	''')
	cursor.execute('''
	CREATE UNIQUE INDEX f_reposts_postnum ON f_reposts(postnum)
	''')
	cursor.execute('''
	CREATE INDEX f_reposts_timestamp ON f_reposts(timestamp)
	''')
	cursor.execute('''
	ANALYZE f_reposts
	''')

	print('set up f_reposts_meta')
	cursor.execute('''
	CREATE INDEX f_reposts_meta_data_filename ON f_reposts_meta(data_filename)
	''')
	cursor.execute('''
	CREATE INDEX f_reposts_meta_filesize ON f_reposts_meta(filesize)
	''')
	cursor.execute('''
	CREATE INDEX f_reposts_meta_first_seen ON f_reposts_meta(first_seen)
	''')
	cursor.execute('''
	CREATE INDEX f_reposts_meta_width_height ON f_reposts_meta(width_px, height_px)
	''')
	cursor.execute('''
	ANALYZE f_reposts_meta
	''')

	print('set up f_comments')
	cursor.execute('''
	CREATE UNIQUE INDEX f_comments_postnum_subnum ON f_comments(postnum, subnum)
	''')
	cursor.execute('''
	CREATE UNIQUE INDEX f_comments_threadnum_postnum_subnum ON f_comments(threadnum, postnum, subnum)
	''')
	cursor.execute('''
	ANALYZE f_comments
	''')

	print('count replies')
	cursor.execute('''
	UPDATE f_reposts AS fr
	SET replies=(
		SELECT COUNT()
		FROM f_comments AS fc
		WHERE
			fc.threadnum=fr.postnum                            -- comments with "fr" as the thread
			AND NOT (fc.postnum=fc.threadnum AND fc.subnum=0)) -- not OP
	''')

	print('commit txn')
	cursor.execute('COMMIT TRANSACTION')
	print('close cursor')
	cursor.close()
	print('commit db')
	db.commit()
	print('close db')
	db.close()

if __name__ == '__main__':
	csv_path = None
	db_path = None
	thumbs_root = None
	for arg in sys.argv[1:]:
		if arg.startswith('-csv='):
			csv_path = arg[5:]
		elif arg.startswith('-db='):
			db_path = arg[4:]
		elif arg.startswith('-thumbs='):
			thumbs_root = arg[8:]
		else:
			print(f'unknown option "{arg}"', file=sys.stderr)
			exit(1)
	if not (csv_path and db_path):
		print(f'usage: {sys.argv[0]} -csv=<path/to/f.csv> -db=<path/to/output.db>', file=sys.stderr)
		exit(1)
	create(csv_path, db_path, thumbs_root)
