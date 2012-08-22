#include <stdio.h>
#include <string.h>
#include <sqlite3.h>
#include <errno.h>
#include <sys/stat.h>
#include "fileset.h"

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
find_by_crc(sqlite3 *db, off_t size, unsigned int crc)
{
	char **table;
	char *errmsg;
	int nrows, ncols;
	int id = -1;

	char *query = sqlite3_mprintf("SELECT id FROM files WHERE size=%d AND crc=%u", size, crc);
	if (sqlite3_get_table(db, query, &table, &nrows, &ncols, &errmsg) != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_free(query);
		sqlite3_close(db);
		return -1;
	}

	if (nrows == 1) {
		sscanf(table[1], "%d", &id);
	} else if (nrows > 1) {
		fprintf(stderr, "Error: multiple size/crc matches\n");
	}
	sqlite3_free_table(table);
	sqlite3_free(query);

	return id;
}

int
make_dirtree(char *path, int make_leaf)
{
	char *ptr = path;
	while ((ptr = strchr(ptr+1, '/')) != NULL) {
		*ptr = '\0';
		if (mkdir(path, S_IRWXU | S_IRWXG | S_IRWXO) == -1 && errno != EEXIST) {
			return -1;
		}
		*ptr = '/';
	}
	if (make_leaf) {
		return mkdir(path, S_IRWXU | S_IRWXG | S_IRWXO);
	}

	return 0;
}

