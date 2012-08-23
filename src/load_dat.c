#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#include "fileset.h"

int
load_csv(char *in_file, char *root, sqlite3 *db)
{
	FILE	*input;
	char	*errmsg = NULL;
	char	*set_name = NULL;
	int	collection_id, set_id;

	if ((input = fopen(in_file, "r")) == NULL) {
		fprintf(stderr, "couldn't open %s\n", in_file);
		return -1;
	}

	const char *prep_stmt;
	sqlite3_stmt *set_stmt, *file_stmt;
	prep_stmt = sqlite3_mprintf("INSERT INTO sets (collection_id, name) VALUES (@CID, @NM)");
	sqlite3_prepare_v2(db, prep_stmt, -1, &set_stmt, NULL);
	sqlite3_free((char *)prep_stmt);
	prep_stmt = sqlite3_mprintf("INSERT INTO files (set_id, name, size, crc, comment) VALUES (@SID, @NM, @SZ, @CRC, @COM)");
	sqlite3_prepare_v2(db, prep_stmt, -1, &file_stmt, NULL);
	sqlite3_free((char *)prep_stmt);

	sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &errmsg);

	char *dot = strrchr(in_file, '.');
	if (dot) {
		*dot = '\0';
	}
	char *fname = strrchr(in_file, '/');
	SQL_INSERT(db, "INSERT INTO collections (name, root) VALUES (%Q, %Q)", fname == NULL ? in_file : fname+1, root);
	collection_id = sqlite3_last_insert_rowid(db);

	char	*line;
	size_t	nchars;
	while (getline(&line, &nchars, input) >= 0) {
		int vcount = 0;
		char *tok;

		char *field[5] = {0};
		while ((tok = strtok(vcount == 0 ? line : NULL, ",\r\n")) != NULL && vcount < 5) {
			int merged = 0;
			if (vcount > 0) {
				int lastchar = strlen(field[vcount-1])-1;
				if ((field[vcount-1][lastchar] == '\\' && field[vcount-1][0] != '\\') || // handle str\,str
				    (field[vcount-1][0] == '"' && field[vcount-1][lastchar] != '"') || // handle "str,str"
				    (field[vcount-1][0] == '\\' && field[vcount-1][lastchar] != '\\') || /* handle \str,str\*/
				    (field[vcount-1][0] == '\'' && field[vcount-1][lastchar] != '\'')) { // handle 'str,str'
					*--tok = ',';
					merged = 1;
				}
			}
			if (merged == 0 && field[vcount] == NULL) {
				field[vcount++] = tok;
			}
		}

		int i;
		for (i = 0; i < vcount; i++) {
			int lastchar = strlen(field[i])-1;
			if (field[i][0] == field[i][lastchar] && (field[i][0] == '\'' || field[i][0] == '"' || field[i][0] == '\\')) {
				field[i][lastchar] = '\0';
				field[i]++;
			}
		}

		char decbuf[32];
		unsigned int hexbuf;
		sscanf(field[2], "%x", &hexbuf);
		sprintf(decbuf, "%u", hexbuf);
		
		if (!set_name || strcmp(set_name, field[3])) {
			free(set_name);
			set_name = strdup(field[3]);
			sqlite3_bind_int(set_stmt, 1, collection_id);
			if (!strcmp(set_name, "\\")) {
				sqlite3_bind_text(set_stmt, 2, "/", 1, SQLITE_STATIC);
			} else {
				sqlite3_bind_text(set_stmt, 2, set_name, -1, NULL);
			}
			sqlite3_step(set_stmt);
			set_id = sqlite3_last_insert_rowid(db);
			sqlite3_reset(set_stmt);
		}

		sqlite3_bind_int(file_stmt, 1, set_id);
		sqlite3_bind_text(file_stmt, 2, field[0], -1, NULL);
		sqlite3_bind_text(file_stmt, 3, field[1], -1, NULL);
		sqlite3_bind_text(file_stmt, 4, decbuf, -1, NULL);
		if (vcount == 5) {
			sqlite3_bind_text(file_stmt, 5, field[4], -1, NULL);
		} else {
			sqlite3_bind_null(file_stmt, 5);
		}
		sqlite3_step(file_stmt);
		sqlite3_reset(file_stmt);

		continue;
	}

	sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errmsg);

	fclose(input);
	return 0;
}

int
load_cmpro_dat(char *in_file, char *root, sqlite3 *db)
{
	FILE	*input;
	char	*errmsg = NULL;

	if ((input = fopen(in_file, "r")) == NULL) {
		fprintf(stderr, "couldn't open %s\n", in_file);
		return -1;
	}


	char	*line;
	size_t	nchars;
	while (getline(&line, &nchars, input)) {
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
					} else if (!strcmp(rtag, "name") || !strcmp(rtag, "description") || !strcmp(rtag, "comment")) {
						strappend(&rvalues, strtok(NULL, "\""));
					} else if (!strcmp(rtag, "crc")) {
						char decbuf[32];
						unsigned int hexbuf;
						sscanf(strtok(NULL, " \""), "%x", &hexbuf);
						sprintf(decbuf, "%u", hexbuf);
						strappend(&rvalues, decbuf);
					} else {
						strappend(&rvalues, strtok(NULL, " \""));
					}
					strappend(&rtags, rtag);
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
			SQL_INSERT(db, "INSERT INTO collections (%s, root) VALUES (%s, %Q)", tags, values, root);
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
	return 0;
}
