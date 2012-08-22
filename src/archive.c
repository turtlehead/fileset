#include <stdio.h>
#include <string.h>
#include <sqlite3.h>

#include "fileset.h"

mz_zip_archive *
open_zip(char *path, int create)
{
	mz_zip_archive	*arc;
	int		status = 0, i;

	arc = (mz_zip_archive *)calloc(1, sizeof(mz_zip_archive));
	if (!(status = mz_zip_reader_init_file(arc, path, 0))) {
		char *zpath = sqlite3_mprintf("%s.zip", path);
		status = mz_zip_reader_init_file(arc, zpath, 0);
		sqlite3_free(zpath);
	}
	if (!status) {
		free(arc);
		arc = NULL;
	}

	return arc;
}

int CALLBACK
rar_extract_to_mem(unsigned int msg, long user, long p1, long p2)
{
	switch (msg) {
	case UCM_CHANGEVOLUME:
		switch (p2) {
		case RAR_VOL_ASK:
			fprintf(stderr, "Next volume in archive (%s) is missing.\n", (char *)p1);
			return -1;
		case RAR_VOL_NOTIFY:
			return 1;
		}
		break;
	case UCM_PROCESSDATA:
		memcpy(*(char **)user, (char *)p1, p2);
		*(char **)user += p2;
		return 1;
		break;
	case UCM_NEEDPASSWORD:
		fprintf(stderr, "Passworded rars aren't supported yet.\n");
		return -1;
	}
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
