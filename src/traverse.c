#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include <sqlite3.h>
#include <mhash.h>

#include "fileset.h"

int
move_file(char *src, char *dest_dir, char *dest_file, void *user, int mode)
{
	char	*dest;

	int in;
	struct stat sb;

	if (mode & ONLY_DELETE) {
		if (mode & DELETE) {
			unlink(src);
		}
		return 0;
	}

	if ((in = open(src, O_RDONLY)) == -1) {
		return -1;
	}
	stat(src, &sb);
	if (mode & ZIP) {
		dest = sqlite3_mprintf("%s.zip", dest_dir);
		char *buffer = (char *)malloc(sb.st_size);
		int count = 0;

		make_dirtree(dest_dir, 0);
		while ((count += read(in, buffer+count, sb.st_size-count)) < sb.st_size);
		if (!mz_zip_add_mem_to_archive_file_in_place(dest, dest_file, buffer, sb.st_size, NULL, 0, MZ_NO_COMPRESSION)) {
			fprintf(stderr, "error: mz_zip_add_mem_to_archive_file_in_place failed, %d\n");
			return -1;
		}
		free(buffer);
	} else {
		dest =  sqlite3_mprintf("%s/%s", dest_dir, dest_file);
		make_dirtree(dest, 0);
		if (!(mode & DELETE) || (rename(src, dest) == -1 && errno == EXDEV)) {
			int out;
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
			close(out);
		}
	}
	
	close(in);
	if (mode & DELETE) {
		unlink(src);
	}
	sqlite3_free(dest);
	return 0;
}


int
move_zip(char *src, char *dest_dir, char *dest_file, void *zi, int mode)
{
	char		*dest;
	struct zipinfo	*zip = (struct zipinfo *)zi;


	if (mode & ONLY_DELETE) {
	} else if (mode & ZIP) {
		dest = sqlite3_mprintf("%s.zip", dest_dir);
		char *buffer = (char *)malloc(zip->stat->m_uncomp_size);

		make_dirtree(dest_dir, 0);
		mz_zip_reader_extract_to_mem(zip->zip, zip->index, buffer, zip->stat->m_uncomp_size, 0);
		if (!mz_zip_add_mem_to_archive_file_in_place(dest, dest_file, buffer, zip->stat->m_uncomp_size, NULL, 0, MZ_NO_COMPRESSION)) {
			fprintf(stderr, "error: mz_zip_add_mem_to_archive_file_in_place failed, %d\n");
			return -1;
		}
		free(buffer);
	} else {
		dest = sqlite3_mprintf("%s/%s", dest_dir, dest_file);
		make_dirtree(dest, 0);
		mz_zip_reader_extract_to_file(zip->zip, zip->index, dest, 0);
	}
	
	sqlite3_free(dest);
	return 0;
}


int
move_rar(char *src, char *dest_dir, char *dest_file, void *ri, int mode)
{
	struct rarinfo	*rar = (struct rarinfo *)ri;
	char		*dest;

	if (mode & ONLY_DELETE) {
	} else if (mode & ZIP) {
		dest = sqlite3_mprintf("%s.zip", dest_dir);
		char *buffer = (char *)malloc(rar->hdr->UnpSize);
		char *bptr = buffer;

		make_dirtree(dest_dir, 0);
		RARSetCallback(rar->rar, rar_extract_to_mem, (long)&bptr);
		RARProcessFile(rar->rar, RAR_EXTRACT, NULL, NULL);
		if (!mz_zip_add_mem_to_archive_file_in_place(dest, dest_file, buffer, rar->hdr->UnpSize, NULL, 0, MZ_NO_COMPRESSION)) {
			fprintf(stderr, "error: mz_zip_add_mem_to_archive_file_in_place failed, %d\n");
			return -1;
		}
		free(buffer);
	} else {
		dest = sqlite3_mprintf("%s/%s", dest_dir, dest_file);
		make_dirtree(dest, 0);
		RARProcessFile(rar->rar, RAR_EXTRACT, NULL, dest);
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
				  "TRIM(s.name || '/' || f.name, '/ '), "
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
		mover(src, table[3], table[4], user, mode);// | ((table[5][0]=='1')?ONLY_DELETE:0));
		dest = sqlite3_mprintf("%s/%s", table[3], table[4]);
		SQL_UPDATE(db, "UPDATE files SET found=1 WHERE id=%d", id);
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
	id = find_by_crc(db, sb.st_size, ihash);
	if (mode & HUNT && id > 0) {
		char *dest = archive_file(db, path, id, &move_file, NULL, mode);
		fprintf(stderr, "Move %s to %s\n", path, dest);
		sqlite3_free(dest);
	}
	if (mode & VERBOSE) {
		fprintf(stdout, "File: %s\t%s\n", path, id>0?"Found":"Unknown");
	}

	return 1;
}


int
verify_zip(sqlite3 *db, char *path, mz_zip_archive *ziparc, int mode)
{
	int i;
	int count = 0;
	int id;

	for (i = 0; i < mz_zip_reader_get_num_files(ziparc); i++) {
		if (mz_zip_reader_is_file_a_directory(ziparc, i)) {
			continue;
		}
		count++;
		mz_zip_archive_file_stat zsb;
		mz_zip_reader_file_stat(ziparc, i, &zsb);
		id = find_by_crc(db, zsb.m_uncomp_size, zsb.m_crc32);
		if (mode & HUNT && id > 0) {
			struct zipinfo zi = {ziparc, &zsb, i};
			char *dest = archive_file(db, path, id, &move_zip, &zi, mode);
			fprintf(stderr, "Move %s to %s\n", path, dest);
			sqlite3_free(dest);
		}
		if (mode & VERBOSE) {
			char buf[MZ_ZIP_MAX_ARCHIVE_FILENAME_SIZE];
			mz_zip_reader_get_filename(ziparc, i, buf, MZ_ZIP_MAX_ARCHIVE_FILENAME_SIZE);
			fprintf(stdout, "ZFile: %s/%s\t%s\n", path, buf, id>0?"Found":"Unknown");
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
		id = find_by_crc(db, hdr.UnpSize, hdr.FileCRC);
		if (mode & SEARCH || mode & VERIFY || mode & COUNT) {
			RARProcessFile(rararc, RAR_SKIP, NULL, NULL);
		} else if (mode & HUNT && id > 0) {
			struct rarinfo ri = {rararc, &hdr};
			char *dest = archive_file(db, path, id, &move_rar, &ri, mode);
			fprintf(stderr, "Move %s to %s\n", path, dest);
			sqlite3_free(dest);
		}
		if (mode & VERBOSE) {
			fprintf(stdout, "RFile: %s/%s\t%s\n", path, hdr.FileName, id>0?"Found":"Unknown");
		}
	}

	return count;
}

int
find(sqlite3 *db, char *path, int mode)
{
	DIR		*dir;
	struct dirent	*ent;
	struct stat	sb;
	mz_zip_archive	*ziparc;
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
					count += mz_zip_reader_get_num_files(ziparc);
				} else {
					count += verify_zip(db, fname, ziparc, mode);
				}
				mz_zip_reader_end(ziparc);
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
			count += mz_zip_reader_get_num_files(ziparc);
		} else {
			count += verify_zip(db, path, ziparc, mode);
		}
		mz_zip_reader_end(ziparc);
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
