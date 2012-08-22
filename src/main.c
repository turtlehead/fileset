#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pwd.h>
#include <errno.h>
#include <sys/stat.h>

#include <sqlite3.h>

#include "fileset.h"

int
main(int argc, char **argv)
{
	sqlite3	*db;
	char	*errmsg = NULL;
	char	*dbname = NULL;
	char	*crcname = NULL;
	char	*root = NULL;
	char	opt;
	int	dat_flag = 0;
	int	zip_flag = 0;
	int	setup_flag = 0;
	int	find_flags = 0;
	FILE *in;

	while ((opt = getopt(argc, argv, "c:d:em:r:svz")) != -1) {
		switch (opt) {
		case 'c':
			dat_flag = CSV;
			crcname = optarg;
			break;
		case 'd':
			dbname = optarg;
			break;
		case 'e':
			find_flags |= DELETE;
			break;
		case 'm':
			dat_flag = CMPRO;
			crcname = optarg;
			break;
		case 'r':
			root = optarg;
			break;
		case 's':
			setup_flag = 1;
			break;
		case 'v':
			find_flags |= VERBOSE;
			break;
		case 'z':
			find_flags |= ZIP;
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
	if (setup_flag) {
		if (unlink(dbname) && errno != ENOENT) {
			perror("clearing old database failed");
			return EXIT_FAILURE;
		}
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
	sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, &errmsg);
	sqlite3_exec(db, "PRAGMA journal_mode = MEMORY", NULL, NULL, &errmsg);

	if (!argv[optind]) {
		return EXIT_SUCCESS;
	} else if (!strcmp(argv[optind], "add")) {
		HANDLE *rararc = NULL;
		char *actual_root = realpath(root, NULL);
		if (actual_root == NULL && errno == ENOENT) {
			make_dirtree(root, 1);
			actual_root = realpath(root, NULL);
		} else {
			fprintf(stderr, "error: couldn't determine actual root path\n");
			return EXIT_FAILURE;
		}
		if ((rararc = rar_open(crcname, 1)) != NULL) {
			for (;;) {
				struct RARHeaderDataEx hdr = {0};
				int retval;
				if ((retval = RARReadHeaderEx(rararc, &hdr)) != 0) {
					break;
				}
				if ((hdr.Flags & 0xe0) == 0xe0) {
					continue;
				}
				RARProcessFile(rararc, RAR_EXTRACT, NULL, hdr.FileName);

				if (dat_flag == CSV) {
					if (load_csv(hdr.FileName, actual_root, db) == EXIT_FAILURE)
					return EXIT_FAILURE;
				} else if (dat_flag == CMPRO) {
					if (load_cmpro_dat(hdr.FileName, actual_root, db) == EXIT_FAILURE)
					return EXIT_FAILURE;
				}
			}
			rar_close(rararc);
		} else {
			if (dat_flag == CSV) {
				if (load_csv(crcname, actual_root, db) == EXIT_FAILURE)
					return EXIT_FAILURE;
			} else if (dat_flag == CMPRO) {
				if (load_cmpro_dat(crcname, actual_root, db) == EXIT_FAILURE)
					return EXIT_FAILURE;
			}
		}
		free(actual_root);
	} else if (!strcmp(argv[optind], "search")) {
		fprintf(stdout, "Searching %d files\n", find(db, ".", COUNT));
		fprintf(stdout, "\r%d files searched\n", find(db, ".", SEARCH | find_flags));
	} else if (!strcmp(argv[optind], "verify")) {
		SQL_UPDATE(db, "UPDATE files SET found=0");
		char *query = sqlite3_mprintf("SELECT name, root FROM collections");
		char **table, *errmsg;
		int nrows, ncols, i;
		if (sqlite3_get_table(db, query, &table, &nrows, &ncols, &errmsg) != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", errmsg);
			sqlite3_free(errmsg);
			sqlite3_free(query);
			sqlite3_close(db);
			return EXIT_FAILURE;
		}
		for (i = ncols; i < ((nrows+1)*ncols); i+=ncols) {
			char *dir = sqlite3_mprintf("%s/%s", table[i+1], table[i]);
			fprintf(stdout, "\r%d files searched\n", find(db, dir, VERIFY | find_flags));
			sqlite3_free(dir);
		}
		sqlite3_free_table(table);
		sqlite3_free(query);
	} else if (!strcmp(argv[optind], "hunt")) {
		find(db, ".", HUNT | find_flags);
	} else if (!strcmp(argv[optind], "list")) {
		char *query = sqlite3_mprintf("SELECT c.name, c.root, (SELECT COUNT(*) FROM sets WHERE collection_id=c.id), COUNT(f.id), SUM(f.found) FROM collections c, sets s, files f WHERE c.id = s.collection_id AND s.id = f.set_id group by c.id");
		char **table, *errmsg;
		int nrows, ncols, i;
		if (sqlite3_get_table(db, query, &table, &nrows, &ncols, &errmsg) != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", errmsg);
			sqlite3_free(errmsg);
			sqlite3_free(query);
			sqlite3_close(db);
			return EXIT_FAILURE;
		}
		fprintf(stdout, "%d Collections:\n", nrows);
		for (i = ncols; i < ((nrows+1)*ncols); i+=ncols) {
			fprintf(stdout, "%s:\t%s -- %s/%s\n", table[i], table[i+2], table[i+4], table[i+3]);
		}
		sqlite3_free_table(table);
		sqlite3_free(query);
	} else {
		fprintf(stderr, "Unknown command %s.\n"
			"search - search local tree for files in db.\n"
			"verify - verify files in collection directories.\n"
			"hunt   - search local tree for files and move"
			"         them into collections", argv[optind]);
	}

	sqlite3_close(db);

	return EXIT_SUCCESS;
}
