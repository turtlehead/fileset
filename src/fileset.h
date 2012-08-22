#ifndef _FILESET_H_
#define _FILESET_H_

#define MINIZ_HEADER_FILE_ONLY

#include "miniz.c"
#include "dll.hpp"

#define COLLECTION 1
#define SET 2

#define CSV 1
#define CMPRO 2

// Commands
#define SEARCH 1
#define VERIFY 2
#define HUNT 4
#define COUNT 8
// Modifiers
#define VERBOSE 16	// Verbose status messages
#define ZIP 32		// Put hunted files in a zip, not a dir
#define DELETE 64	// Move found files, don't copy
#define ONLY_DELETE 128	// Don't try to move, only delete if DELETE is set

#define CREATE_COLLECTIONS \
"CREATE TABLE IF NOT EXISTS collections (id INTEGER PRIMARY KEY AUTOINCREMENT," \
					"name VARCHAR," \
					"root VARCHAR," \
					"description VARCHAR," \
					"version VARCHAR," \
					"comment VARCHAR," \
					"header VARCHAR)"
#define CREATE_SETS \
"CREATE TABLE IF NOT EXISTS sets (id INTEGER PRIMARY KEY AUTOINCREMENT," \
				 "collection_id INTEGER," \
				 "name VARCHAR," \
				 "description VARCHAR)"
#define CREATE_FILES \
"CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT," \
				  "set_id INTEGER," \
				  "name VARCHAR," \
				  "size INTEGER," \
				  "flags VARCHAR," \
				  "crc UNSIGNED INTEGER," \
				  "md5 CHARACTER(32)," \
				  "sha1 CHARACTER(40)," \
				  "comment VARCHAR," \
				  "found INTEGER DEFAULT 0)"

#define SQL_INSERT(db, ...) { \
	char *query = sqlite3_mprintf(__VA_ARGS__); \
	char *errmsg = NULL; \
	if (sqlite3_exec(db, query, NULL, 0, &errmsg) != SQLITE_OK) { \
		fprintf(stderr, "SQL error: %s\n", errmsg); \
		sqlite3_free(errmsg); \
		sqlite3_free(query); \
		sqlite3_close(db); \
		exit(-1); \
	} \
	sqlite3_free(query); \
}

#define SQL_UPDATE(db, ...) SQL_INSERT(db, __VA_ARGS__)

struct zipinfo {
	mz_zip_archive			*zip;
	mz_zip_archive_file_stat	*stat;
	int				index;
};

struct rarinfo {
	HANDLE			rar;
	struct RARHeaderDataEx	*hdr;
};

mz_zip_archive *open_zip(char *, int);
int CALLBACK rar_extract_to_mem(unsigned int, long, long, long);
HANDLE rar_open(char *, int);
void rar_close(HANDLE);
int rar_get_num_files(HANDLE);

int load_csv(char *, char *, sqlite3 *);
int load_cmpro_dat(char *, char *, sqlite3 *);

int find(sqlite3 *, char *, int);

#endif