#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pwd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include <mhash.h>
#include <sqlite3.h>
#include <zip.h>

#define COLLECTION 1
#define SET 2
#define CSV 1
#define CMPRO 2

#define CREATE_COLLECTIONS \
"CREATE TABLE IF NOT EXISTS collections (id INTEGER PRIMARY KEY AUTOINCREMENT," \
				    "name VARCHAR," \
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
				 "crc INTEGER," \
				 "md5 CHARACTER(32)," \
				 "sha1 CHARACTER(40)," \
				 "comment VARCHAR)"

#define SQL_INSERT(db, ...) { \
	char *query = sqlite3_mprintf(__VA_ARGS__); \
	char *errmsg = NULL; \
	if (sqlite3_exec(db, query, NULL, 0, &errmsg) != SQLITE_OK) { \
		fprintf(stderr, "SQL error: %s\n", errmsg); \
		sqlite3_free(errmsg); \
		sqlite3_free(query); \
		sqlite3_close(db); \
		return EXIT_FAILURE; \
	} \
	sqlite3_free(query); \
}

char *
strappend(char **str, char *frag)
{
	if (*str == NULL) {
		*str = sqlite3_mprintf("%Q", frag);
	} else {
		*str = sqlite3_mprintf("%z, %Q", *str, frag);
	}

	return *str;
}

int
find(sqlite3 *db, char *path)
{
	DIR		*dir;
	struct dirent	*ent;
	struct stat	sb;
	int		pathlen = strlen(path);
	int		i, j;

	if ((dir = opendir(path)) == NULL) {
		return EXIT_FAILURE;
	}
	while ((ent = readdir(dir)) != NULL) {
		if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) {
			continue;
		}
		char *fname = (char *)calloc(pathlen + strlen(ent->d_name) +
					     2, sizeof(char));
		strcat(strcat(strcpy(fname, path), "/"), ent->d_name);
		stat(fname, &sb);
		if (S_ISDIR(sb.st_mode)) {
			find(db, fname);
			free(fname);
			continue;
		} else if (S_ISLNK(sb.st_mode)) {
			char		*realname = realpath(fname, NULL);
			DIR		*ldir;
			struct stat	lsb;

			free(fname);
			if (stat(realname, &lsb) == 0 
			    && S_ISDIR(lsb.st_mode)) {
				find(db, realname);
				free(realname);
				continue;
			} else {
				fname = realname;
			}
		}

		MHASH td;
		unsigned char hash[20], chash[41] = {0};
		char **table;
		char *errmsg;
		int nrows, ncols;

		if (!strcmp(strrchr(fname, '.'), ".zip")) {
			struct zip *zarc = zip_open(fname, 0, NULL);
			unsigned char buffer[1024];
			for (i = 0; i < zip_get_num_files(zarc); i++) {
				int len;
				struct zip_file *zfile = zip_fopen_index(zarc,i, 0);
				struct zip_stat zsb;
				zip_stat_index(zarc, i, 0, &zsb);
				if (zsb.size == 0 && zsb.crc == 0 
				    && zsb.name[strlen(zsb.name)-1] == '/') {
					zip_fclose(zfile);
					continue;
				}
				zip_fclose(zfile);
				fprintf(stdout, "ZFile: %s/%s\t%d\t%.8x\n", fname, zip_get_name(zarc, i, 0), zsb.size, zsb.crc);
				char *query = sqlite3_mprintf("SELECT count(id) FROM files WHERE size=%d AND crc LIKE '%x'", zsb.size, zsb.crc);
				if (sqlite3_get_table(db, query, &table, &nrows, &ncols, &errmsg) != SQLITE_OK) {
					fprintf(stderr, "SQL error: %s\n", errmsg);
					sqlite3_free(errmsg);
					sqlite3_free(query);
					sqlite3_close(db);
					return EXIT_FAILURE;
				}
				fprintf(stdout, "\tcount = %s\n", table[1]);
				sqlite3_free_table(table);
				sqlite3_free(query);
			}
			zip_close(zarc);
		} else {
			int in;

			if ((in = open(fname, O_RDONLY, 0)) == -1) {
				return EXIT_FAILURE;
			}
			if ((td = mhash_init(MHASH_CRC32B)) == MHASH_FAILED) {
				return EXIT_FAILURE;
			}
			unsigned char *buffer;
			buffer = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, in, 0);
			mhash(td, buffer, sb.st_size);
			mhash_deinit(td, hash);
			unsigned int ihash = (hash[3] << 24) + (hash[2] << 16) + (hash[1] << 8) + hash[0];

			fprintf(stdout, "File: %s\t%d\t", fname, sb.st_size);
			fprintf(stdout, "%x\n", ihash);

			char *query = sqlite3_mprintf("SELECT count(id) FROM files WHERE size=%d AND crc LIKE '%x'", sb.st_size, ihash);
			if (sqlite3_get_table(db, query, &table, &nrows, &ncols, &errmsg) != SQLITE_OK) {
				fprintf(stderr, "SQL error: %s\n", errmsg);
				sqlite3_free(errmsg);
				sqlite3_free(query);
				sqlite3_close(db);
				return EXIT_FAILURE;
			}
			fprintf(stdout, "\tcount = %s\n", table[1]);
			sqlite3_free_table(table);
			sqlite3_free(query);
		}

		free(fname);
	}
	closedir(dir);

	return EXIT_SUCCESS;
}

// Still needs optimization & error handling.
int
load_csv(char *in_file, sqlite3 *db)
{
	FILE	*input;
	char	*errmsg = NULL;
	char	*set_name = NULL;
	int	collection_id, set_id;

	if ((input = fopen(in_file, "r")) == NULL) {
		fprintf(stderr, "couldn't open %s\n", in_file);
		return EXIT_FAILURE;
	}

	char *dot = strrchr(in_file, '.');
	if (dot) {
		*dot = '\0';
	}
	SQL_INSERT(db, "INSERT INTO collections (name) VALUES (%Q)", in_file);
	collection_id = sqlite3_last_insert_rowid(db);

	char line[2048];
	while (fgets(line, 2048, input)) {
		int vcount = 0;
		char *values = NULL, *tok;
		char *query;

		fprintf(stdout, "Line: %s\n", line);
		char *name = strtok(line, "\"");
		strappend(&values, name);
		while ((tok = strtok(NULL, ",\r\n")) != NULL) {
			if (vcount == 2) {
				if (!set_name || strcmp(set_name, tok)) {
					free(set_name);
					set_name = strdup(tok);
					SQL_INSERT(db, "INSERT INTO sets (collection_id, name) VALUES (%d, %Q)", collection_id, set_name);
					set_id = sqlite3_last_insert_rowid(db);
				}
			} else {
				strappend(&values, tok);
			}
			vcount++;
		}
		if (vcount == 5) {
			SQL_INSERT(db, "INSERT INTO files (set_id, name, size, crc, comment) VALUES (%d, %s)", set_id, values);
		} else {
			SQL_INSERT(db, "INSERT INTO files (set_id, name, size, crc) VALUES (%d, %s)", set_id, values);
		}
		sqlite3_free(values);
	}

	fclose(input);
	return EXIT_SUCCESS;
}

// Still needs header handling, other fields.
// Also optimization & error handling.
int
load_cmpro_dat(char *in_file, sqlite3 *db)
{
	FILE	*input;
	char	*errmsg = NULL;

	if ((input = fopen(in_file, "r")) == NULL) {
		fprintf(stderr, "couldn't open %s\n", in_file);
		return EXIT_FAILURE;
	}

	char line[2048];
	while (fgets(line, 2048, input)) {
		char *tags = NULL, *values = NULL, *files = NULL;
		char *query;
		int table, collection_id;

		char *stype = strtok(line, " \t");
		if (!strcmp(stype, "clrmamepro")) {
			table = COLLECTION;
		} else if (!strcmp(stype, "game")) {
			table = SET;
		} else {
			continue;
		}
		while (strncmp(fgets(line, 2048, input), ")", 1)) {
			char *tag = strtok(line, " \t");
			char rowid[32];
			if (!strcmp(tag, "rom")) {
				char *rtag, *rtags = NULL, *rvalues = NULL;
				while ((rtag = strtok(NULL, " \t")) != NULL) {
					if (!strcmp(rtag, "(") || !strncmp(rtag, ")", 1)) {
						continue;
					} else if (!strcmp(rtag, "name")) {
						strappend(&rtags, rtag);
						strappend(&rvalues, strtok(NULL, "\""));
					} else {
						strappend(&rtags, rtag);
						strappend(&rvalues, strtok(NULL, " \""));
					}
				}

				SQL_INSERT(db, "INSERT INTO files (%s) VALUES (%s)", rtags, rvalues);
				sprintf(rowid, "%d", sqlite3_last_insert_rowid(db));
				strappend(&files, rowid);
				sqlite3_free(rtags);
				sqlite3_free(rvalues);
			} else {
				strappend(&tags, tag);
				strappend(&values, strtok(NULL, "\"\n"));
			}
		}
		if (table == COLLECTION) {
			SQL_INSERT(db, "INSERT INTO collections (%s) VALUES (%s)", tags, values);
			collection_id = sqlite3_last_insert_rowid(db);
		} else {
			SQL_INSERT(db, "INSERT INTO sets (collection_id, %s) VALUES (%d, %s)", tags, collection_id, values);
		}
		sqlite3_free(tags);
		sqlite3_free(values);

		if (files != NULL) {
			SQL_INSERT(db, "UPDATE files SET set_id=%d WHERE id IN (%s)", sqlite3_last_insert_rowid(db), files);
			sqlite3_free(files);
		}
	}

	fclose(input);
	return EXIT_SUCCESS;
}

int
main(int argc, char **argv)
{
	sqlite3	*db;
	char	*errmsg = NULL;
	char	*dbname = NULL;
	char	*crcname = NULL;
	char	opt;
	int	dat_flag = 0;
	FILE *in;

	while ((opt = getopt(argc, argv, "c:d:m:")) != -1) {
		switch (opt) {
		case 'c':
			dat_flag = CSV;
			crcname = optarg;
			break;
		case 'd':
			dbname = optarg;
			break;
		case 'm':
			dat_flag = CMPRO;
			crcname = optarg;
			break;
		}
	}

	if (dbname == NULL) {
		char *home;
		if ((home = getenv("HOME")) == NULL) {
			struct passwd *pw = getpwuid(getuid());
			home = strdup(pw->pw_dir);
		}
		dbname = (char *)calloc(strlen(home)+1+11+1, sizeof(char));
		sprintf(dbname, "%s/.fileset.db", home);
	}

	if (sqlite3_open(dbname, &db)) {
		fprintf(stderr, "couldn't open database: %s\n", sqlite3_errmsg(db));
		sqlite3_close(db);
		return EXIT_FAILURE;
	}

	if (sqlite3_exec(db, CREATE_COLLECTIONS, NULL, 0, &errmsg) != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db);
		return EXIT_FAILURE;
	}
	if (sqlite3_exec(db, CREATE_SETS, NULL, 0, &errmsg) != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db);
		return EXIT_FAILURE;
	}
	if (sqlite3_exec(db, CREATE_FILES, NULL, 0, &errmsg) != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db);
		return EXIT_FAILURE;
	}

	if (dat_flag == CSV) {
		if (load_csv(crcname, db) == EXIT_FAILURE)
			return EXIT_FAILURE;
	} else if (dat_flag == CMPRO) {
		if (load_cmpro_dat(crcname, db) == EXIT_FAILURE)
			return EXIT_FAILURE;
	}

	return find(db, ".");

//Should call sqlite3_close(db) somewhere
}
