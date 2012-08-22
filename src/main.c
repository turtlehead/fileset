#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pwd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include <mhash.h>
#include <sqlite3.h>
#include <zip.h>

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
	struct zip	*zarc;
	struct zip_file	*zfile;
	int		index;
};

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

struct zip *
open_zip(char *path, int create)
{
	struct zip	*arc = NULL;
	int		error = 0;

	if ((arc = zip_open(path, 0, &error)) == NULL) {
		char *zpath = sqlite3_mprintf("%s.zip", path);
		arc = zip_open(zpath, create == 1 ? ZIP_CREATE : 0, &error);
		sqlite3_free(zpath);
	}

	return arc;
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

int
move_file(char *src, char *dest_dir, char *dest_file, void *user, int mode)
{
	char	*dest = sqlite3_mprintf("%s/%s", dest_dir, dest_file);

	if (mode & ONLY_DELETE) {
		if (mode & DELETE) {
			unlink(src);
		}
	} else if (mode & ZIP) {
		make_dirtree(dest_dir, 0);
		struct zip	*zip;
		if ((zip = open_zip(dest_dir, 1)) != NULL) {
			struct zip_source *s = zip_source_file(zip, src, 0, 0);
			if (zip_add(zip, dest_file, s) == -1) {
				fprintf(stderr, "zip error: %s\n", zip_strerror(zip));
			}
			zip_close(zip);
		}
		if (mode & DELETE) {
			unlink(src);
		}
	} else {
		make_dirtree(dest, 0);
		if (!(mode & DELETE) || (rename(src, dest) == -1 && errno == EXDEV)) {
			int in, out;
			struct stat sb;
			if ((in = open(src, O_RDONLY)) == -1) {
				return -1;
			}
			stat(src, &sb);
			if ((out = open(dest, O_RDWR | O_CREAT | O_TRUNC, sb.st_mode)) == -1) {
				return -1;
			}
			pwrite(out, "", 1, sb.st_size - 1);
			unsigned char *srcbuf, *destbuf;
			srcbuf = (unsigned char *)mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, in, 0);
			destbuf = (unsigned char *)mmap(NULL, sb.st_size, PROT_WRITE, MAP_SHARED, out, 0);
			memcpy(destbuf, srcbuf, sb.st_size);
			munmap(srcbuf, sb.st_size);
			munmap(destbuf, sb.st_size);
			close(in);
			close(out);
			if (mode & DELETE) {
				unlink(src);
			}
		}
	}
	
	sqlite3_free(dest);
	return 0;
}

int
move_zip(char *src, char *dest_dir, char *dest_file, void *zi, int mode)
{
	char	*dest = sqlite3_mprintf("%s/%s", dest_dir, dest_file);
	struct zipinfo *zinfo = (struct zipinfo *)zi;


	if (mode & ONLY_DELETE) {
	} else if (mode & ZIP) {
		make_dirtree(dest_dir, 0);
		struct zip	*zip;
		if ((zip = open_zip(dest_dir, 1)) != NULL) {
			struct zip_source *s = zip_source_zip(zip, zinfo->zarc, zinfo->index, 0, 0, 0);
			if (zip_add(zip, dest_file, s) == -1) {
				fprintf(stderr, "zip error: %s\n", zip_strerror(zip));
			}
			zip_close(zip);
		}
	} else {
		make_dirtree(dest, 0);
		int out;
		if ((out = open(dest, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO)) == -1) {
			return -1;
		}
		int bytes; 
		char buffer[1024];
		while ((bytes = zip_fread(zinfo->zfile, buffer, 1024)) > 0) {
			write(out, buffer, bytes);
		}
		close(out);
	}
	
	sqlite3_free(dest);
	return 0;
}

int
move_rar(char *src, char *dest_dir, char *dest_file, void *rar, int mode)
{
	char	*dest = sqlite3_mprintf("%s/%s", dest_dir, dest_file);

	if (mode & ONLY_DELETE) {
	} else if (mode & ZIP) {
		make_dirtree(dest_dir, 0);
		struct zip	*zip;
		if ((zip = open_zip(dest_dir, 1)) != NULL) {
			char tmp[32] = "/tmp/filesetXXXXXX";
			mkstemp(tmp);
			RARProcessFile(rar, RAR_EXTRACT, NULL, tmp);
			struct zip_source *s = zip_source_file(zip, tmp, 0, 0);
			if (zip_add(zip, dest_file, s) == -1) {
				fprintf(stderr, "zip error: %s\n", zip_strerror(zip));
			}
			zip_close(zip);
			unlink(tmp);
		}
	} else {
		make_dirtree(dest, 0);
		RARProcessFile(rar, RAR_EXTRACT, NULL, dest);
	}
	
	sqlite3_free(dest);
	return 0;
}

char *
archive_file(sqlite3 *db, char *src, int id, int (*mover)(char *, char *, char *, void *, int), void *user, int mode)
{
	char **table;
	char *query;
	char *errmsg;
	char *dest = NULL;
	int nrows, ncols;

	query = sqlite3_mprintf("SELECT RTRIM(c.root, '/ ') || '/' || TRIM(c.name, '/ '), "
				  "RTRIM(s.name, '/ ') || '/' || f.name, "
				  "f.found "
				"FROM collections c, sets s, files f "
				"WHERE c.id = s.collection_id "
				  "AND s.id = f.set_id "
				  "AND f.id = %d", id);
	if (sqlite3_get_table(db, query, &table, &nrows, &ncols, &errmsg) != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_free(query);
		sqlite3_close(db);
		return NULL;
	}
	if (nrows == 1) {
		mover(src, table[3], table[4], user, mode | ((table[5][0]=='1')?ONLY_DELETE:0));
		dest = sqlite3_mprintf("%s/%s", table[2], table[3]);
	} else if (nrows > 1) {
		fprintf(stderr, "Error: multiple size/crc matches\n");
	}
	sqlite3_free_table(table);
	sqlite3_free(query);

	return dest;
}

int
verify_file(sqlite3 *db, char *path, struct stat sb, int mode)
{
	MHASH td;
	unsigned char hash[20];
	int id;
	int in;

	if ((in = open(path, O_RDONLY, 0)) == -1) {
		return 0;
	}
	if ((td = mhash_init(MHASH_CRC32B)) == MHASH_FAILED) {
		return 0;
	}
	unsigned char *buffer;
	buffer = (unsigned char *)mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, in, 0);
	mhash(td, buffer, sb.st_size);
	mhash_deinit(td, hash);
	munmap(buffer, sb.st_size);
	close(in);
	unsigned int ihash = (hash[3] << 24) + (hash[2] << 16) + (hash[1] << 8) + hash[0];
	if (mode & HUNT && id > 0) {
		char *dest = archive_file(db, path, id, &move_file, NULL, mode);
		fprintf(stderr, "Move %s to %s\n", path, dest);
		sqlite3_free(dest);
	}
	if ((id = find_by_crc(db, sb.st_size, ihash)) > 0) {
		SQL_UPDATE(db, "UPDATE files SET found=1 WHERE id=%d", id);
	}
	if (mode & VERBOSE) {
		fprintf(stdout, "File: %s\t%s\n", path, id>0?"Found":"Unknown");
	}

	return 1;
}

int
verify_zip(sqlite3 *db, char *path, struct zip *ziparc, int mode)
{
	int i;
	int count = 0;
	int id;

	for (i = 0; i < zip_get_num_files(ziparc); i++) {
		struct zip_file *zfile = zip_fopen_index(ziparc,i, 0);
		struct zip_stat zsb;
		zip_stat_index(ziparc, i, 0, &zsb);
		if (zsb.size == 0 && zsb.crc == 0 
		    && zsb.name[strlen(zsb.name)-1] == '/') {
			zip_fclose(zfile);
			continue;
		}
		count++;
		if (mode & HUNT) {
			struct zipinfo zi = {ziparc, zfile, i};
			char *dest = archive_file(db, path, id, &move_zip, &zi, mode);
			fprintf(stderr, "Move %s to %s\n", path, dest);
			sqlite3_free(dest);
		}
		zip_fclose(zfile);
		if ((id = find_by_crc(db, zsb.size, zsb.crc)) > 0) {
			SQL_UPDATE(db, "UPDATE files SET found=1 WHERE id=%d", id);
		}
		if (mode & VERBOSE) {
			fprintf(stdout, "ZFile: %s/%s\t%s\n", path, zip_get_name(ziparc, i, 0), id>0?"Found":"Unknown");
		}
	}

	return count;
}

int
verify_rar(sqlite3 *db, char *path, HANDLE *rararc, int mode)
{
	int i;
	int id;

	int count = 0;

	for (;;) {
		struct RARHeaderDataEx hdr = {0};
		int retval;
		if ((retval = RARReadHeaderEx(rararc, &hdr)) != 0) {
			break;
		}
		if ((hdr.Flags & 0xe0) == 0xe0) {
			continue;
		}
		count++;
		if (mode & SEARCH || mode & VERIFY || mode & COUNT) {
			RARProcessFile(rararc, RAR_SKIP, NULL, NULL);
		} else if (mode & HUNT) {
			char *dest = archive_file(db, path, id, &move_rar, rararc, mode);
			fprintf(stderr, "Move %s to %s\n", path, dest);
			sqlite3_free(dest);
		}
		if ((id = find_by_crc(db, hdr.UnpSize, hdr.FileCRC)) > 0) {
			SQL_UPDATE(db, "UPDATE files SET found=1 WHERE id=%d", id);
		}
		if (mode & VERBOSE) {
			fprintf(stdout, "RFile: %s/%s\t%s\n", path, hdr.FileName, id>0?"Found":"Unknown");
		}
	}

	return count;
}

HANDLE
rar_open(char *path, int extract)
{
	HANDLE arc;
	struct RAROpenArchiveDataEx in = {0};

	in.ArcName = path;
	if (extract) {
		in.OpenMode = RAR_OM_EXTRACT;
	} else {
		in.OpenMode = RAR_OM_LIST;
	}

	if ((arc = RAROpenArchiveEx(&in)) == NULL) {
		char *rpath = sqlite3_mprintf("%s.rar", path);
		in.ArcName = rpath;
		arc = RAROpenArchiveEx(&in);
		sqlite3_free(rpath);
	}

	return arc;
}

void
rar_close(HANDLE rararc)
{
	RARCloseArchive(rararc);
}

int
rar_get_num_files(HANDLE rararc)
{
	int count = 0;

	for (;;) {
		struct RARHeaderDataEx hdr = {0};
		int retval;
		if ((retval = RARReadHeaderEx(rararc, &hdr)) != 0) {
			break;
		}
		RARProcessFile(rararc, RAR_SKIP, NULL, NULL);
		count++;
	}

	return count;
}

int
find(sqlite3 *db, char *path, int mode)
{
	DIR		*dir;
	struct dirent	*ent;
	struct stat	sb;
	struct zip	*ziparc;
	HANDLE		rararc;
	int		pathlen = strlen(path);
	int		i, j;
	int		count = 0;

	if ((dir = opendir(path)) != NULL) {
		while ((ent = readdir(dir)) != NULL) {
			if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) {
				continue;
			}
			char *fname = (char *)calloc(pathlen + strlen(ent->d_name) +
						     2, sizeof(char));
			strcat(strcat(strcpy(fname, path), "/"), ent->d_name);
			stat(fname, &sb);
			if (S_ISDIR(sb.st_mode)) {
				count += find(db, fname, mode);
				free(fname);
				continue;
			} else if (S_ISLNK(sb.st_mode)) {
				char		*realname = realpath(fname, NULL);
				DIR		*ldir;
				struct stat	lsb;

				free(fname);
				if (stat(realname, &lsb) == 0 
				    && S_ISDIR(lsb.st_mode)) {
					count +=find(db, realname, mode);
					free(realname);
					continue;
				} else {
					fname = realname;
				}
			} else if ((ziparc = open_zip(fname, 0)) != NULL) {
				if (mode & COUNT) {
					count += zip_get_num_files(ziparc);
				} else {
					count += verify_zip(db, fname, ziparc, mode);
				}
				zip_close(ziparc);
			} else if ((rararc = rar_open(fname, mode & HUNT)) != NULL) {
				if (mode & COUNT) {
					count += rar_get_num_files(rararc);
				} else {
					count += verify_rar(db, fname, rararc, mode);
				}
				rar_close(rararc);
			} else {
				count++;
				if (!(mode & COUNT)) {
					verify_file(db, fname, sb, mode);
				}
			}
			free(fname);
			if (!(mode & COUNT) && !(mode & VERBOSE)) {
				fprintf(stdout, "\r%d files searched", count);
			}
		}
		closedir(dir);
	} else if ((ziparc = open_zip(path, 0)) != NULL) {
		if (mode & COUNT) {
			count += zip_get_num_files(ziparc);
		} else {
			count += verify_zip(db, path, ziparc, mode);
		}
		zip_close(ziparc);
	} else if ((rararc = rar_open(path, mode & HUNT)) != NULL) {
		if (mode & COUNT) {
			count += rar_get_num_files(rararc);
		} else {
			count += verify_rar(db, path, rararc, mode);
		}
		rar_close(rararc);
	} else {
		count++;
		if (!(mode & COUNT)) {
			stat(path, &sb);
			verify_file(db, path, sb, mode);
		}
	}

	return count;
}

// Still needs optimization & error handling.
int
load_csv(char *in_file, char *root, sqlite3 *db)
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
	char *fname = strrchr(in_file, '/');
	SQL_INSERT(db, "INSERT INTO collections (name, root) VALUES (%Q, %Q)", fname == NULL ? in_file : fname+1, root);
	collection_id = sqlite3_last_insert_rowid(db);

	char line[2048];
	while (fgets(line, 2048, input)) {
		int vcount = 0;
		char *values = NULL, *tok;
		char *query;

		// need to filter out lone \ chars
		char *name = strtok(line, "\",");
		if (!strcmp(name, "\\") || name[0] == '\0') {
			strappend(&values, "/");
		} else {
			strappend(&values, name);
		}
		while ((tok = strtok(NULL, ",\r\n")) != NULL) {
			if (vcount == 1) {
				char decbuf[32];
				unsigned int hexbuf;
				sscanf(tok, "%x", &hexbuf);
				sprintf(decbuf, "%u", hexbuf);
				strappend(&values, decbuf);
			} else if (vcount == 2) {
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
		if (vcount == 4) {
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
load_cmpro_dat(char *in_file, char *root, sqlite3 *db)
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
	return EXIT_SUCCESS;
}

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
