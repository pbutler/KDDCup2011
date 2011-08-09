#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <ext/hash_map>
#include <pthread.h>
#include <assert.h>

#include "data.h"

unsigned int nUsers = 0;
unsigned int nItems = 0;
unsigned int nRatings = 0;
unsigned int nValidations =0;
unsigned int nTests = 0;
unsigned int nGenres = 0;
unsigned int nAlbums = 0;
unsigned int nArtists = 0;

char tmpdir[256] = "tmp/";

hash_map<int, int> genreMap;
hash_map<int, int> artistMap;
hash_map<int, int> albumMap;
hash_multimap<int, int> genreItemMap;
unsigned int umaplen = 0,imaplen = 0;
hash_map<int, int> imap;
hash_map<int, int> umap;
struct rating_s *ratings;
struct item_s *items;
struct user_s *users;
struct rating_s *validations;
struct rating_s *tests;
double mu;

inline int parseInt(char *s, ssize_t *i, int *a, ssize_t size) {
	*a = 0;
	while(*i < size) {
		char c = s[*i];
		if (! isdigit(c)) {
			(*i)++;
			break;
		}
		*a = (*a) * 10 + (int)( c -'0');
		(*i)++;
	}
	return *i == size;
}

void *open_read(const char *file, ssize_t *size)
{
	struct stat sbuf;
	int f = open(file, O_RDONLY);
	stat(file, &sbuf);
	*size = sbuf.st_size; // 5967164350;
	//printf("%s %ld\n", file, *size);
	void *data = mmap(0, *size, PROT_READ, MAP_SHARED, f, 0);
	if(data == MAP_FAILED) {
		printf("%s %ld %d\n", file, *size, errno);
		if ( errno == EACCES) printf("ACCESS\n");
		if ( errno == EBADF) printf("BADF\n");
		if ( errno == EINVAL) printf("INVAL\n");
		return NULL;
	}
	close(f);
	madvise(data, size, MADV_SEQUENTIAL);
	return data;
}

void *open_rw(const char *file1, ssize_t size, int advice /*= MADV_SEQUENTIAL*/)
{
	struct stat sbuf;
	char file[256];
	strcpy(file, tmpdir);
	strcat(file, file1);
	int f = open(file, O_APPEND|O_RDWR|O_CREAT, 0664);
	ftruncate(f, size);

	void *data = mmap(0, size, PROT_READ|PROT_WRITE, MAP_SHARED, f, 0);
	if(data == MAP_FAILED) {
		printf("%s %ld %d\n", file, size, errno);
		if ( errno == EACCES) printf("ACCESS\n");
		if ( errno == EBADF) printf("BADF\n");
		if ( errno == EINVAL) printf("INVAL\n");
		return NULL;
	}
	madvise(data, size, advice);
	return data;
}


void read_tests()
{
	int uid, n, iid, stmp, day, hr, min, sec;
	double  score;
	ssize_t didx = 0;
	unsigned int uidx=0, ridx=0, iidx=0;
	ssize_t size;
	char *data = (char *)open_read("testIdx1.txt", &size);
	int sum = 0;
	while(1) {
		if ( parseInt(data, &didx, &uid, size) ) {
			break;
		}
		uidx = umap[uid];
		parseInt(data, &didx, &n, size);
		if (n != 6) printf("Error size not 6!=%d\n", n);
		//users[uidx].qu = n;
		for(int i =0; i < n; i++) {
			parseInt(data, &didx, &iid, size);
			parseInt(data, &didx, &day, size);
			parseInt(data, &didx, &hr, size);
			parseInt(data, &didx, &min, size);
			parseInt(data, &didx, &sec, size);
			day -= 2672;
			if( imap.count(iid) == 0) {
				iidx = imaplen;
				imaplen ++;
				imap[iid] = iidx;

				items[iidx].id = iid;
				items[iidx].count = 0;
				items[iidx].albumid = -1;
				items[iidx].artistid = -1;
			} else {
				iidx = imap[iid];
				assert(items[iidx].id == iid);
			}
			tests[ridx].item = iidx;
			tests[ridx].rating = 0;
			tests[ridx].day = day;
			ridx++;
		}
	}
	munmap(data, size);
	nTests = ridx;
	printf("nTests = %d %d\n", nTests, ridx);
}


void read_validate()
{
	int uid, n, iid, stmp, day, hr, min, sec;
	double  score;
	ssize_t didx = 0;
	unsigned int uidx=0, ridx=0, iidx=0;
	ssize_t size;
	char *data = (char *)open_read("validationIdx1.txt", &size);
	int sum = 0;
	while(1) {
		if ( parseInt(data, &didx, &uid, size) ) {
			break;
		}
		uidx = umap[uid];
		parseInt(data, &didx, &n, size);
		//users[uidx].nu = n;
		for(int i = 0; i < n; i++) {
			parseInt(data, &didx, &iid, size);
			parseInt(data, &didx, &stmp, size);
			parseInt(data, &didx, &day, size);
			parseInt(data, &didx, &hr, size);
			parseInt(data, &didx, &min, size);
			parseInt(data, &didx, &sec, size);
			score =  (double)stmp / SCORENORM;
			day -= 2672;
			if( imap.count(iid) == 0) {
				iidx = imaplen;
				imaplen ++;
				imap[iid] = iidx;

				items[iidx].id = iid;
				items[iidx].count = 0;
				items[iidx].artistid = -1;
				items[iidx].albumid = -1;
			} else {
				iidx = imap[iid];
				assert(items[iidx].id == iid);
			}
			validations[ridx].item = iidx;
			validations[ridx].rating = score;
			validations[ridx].day = day;
			ridx++;
		}
	}
	munmap(data, size);
	nValidations = ridx;
	printf("nValidations = %d\n", nValidations);
}

void read_data()
{
	int uid, n, iid, stmp, day, hr, min, sec;
	double  score;
	ssize_t didx = 0;
	unsigned int uidx=0, ridx=0, iidx=0;
	ssize_t size;
	char *data = (char *)open_read("trainIdx1.txt", &size);
	int sum = 0;
	int start = 0;
	mu = 0;
	while(1) {
		if ( parseInt(data, &didx, &uid, size) ) {
			break;
		}
		//printf("' %c\n", data[didx-1]);
		if (umap.count(uid) == 0) {
			uidx = umaplen;
			umaplen++;
			umap[uid] = uidx;
			users[uidx].id = uid;
		} else {
			uidx = imap[uid];
		}
		parseInt(data, &didx, &n, size);
		users[uidx].count = n;
		users[uidx].start = start;
		start += n;
		for(int i =0; i < n; i++) {
			parseInt(data, &didx, &iid, size);
			parseInt(data, &didx, &stmp, size);
			parseInt(data, &didx, &day, size);
			parseInt(data, &didx, &hr, size);
			parseInt(data, &didx, &min, size);
			parseInt(data, &didx, &sec, size);
			score = (double) stmp / SCORENORM;
			day -= 2672;
			mu += score;
			if( imap.count(iid) == 0) {
				iidx = imaplen;
				imaplen ++;
				imap[iid] = iidx;

				items[iidx].id = iid;
				items[iidx].count = 0;
				items[iidx].artistid = -1;
				items[iidx].albumid = -1;
			} else {
				iidx = imap[iid];
				assert(items[iidx].id == iid);
			}
			items[iidx].count += 1;
			ratings[ridx].item = iidx;
			ratings[ridx].rating = score;
			ratings[ridx].day = day;
			assert(ratings[ridx].rating <= 100);
			ridx++;
		}
	}
	munmap(data, size);
	read_validate();
	read_tests();
	nItems = imaplen;
	nUsers = umaplen;
	nRatings = ridx;
	mu /= (double)ridx;
	printf("nRatings = %d\n", nRatings);
	printf("mu = %lg\n", mu);

	char vfile[256];
	strcpy(vfile, tmpdir);
	strcat(vfile,"values");
	FILE *tmp = fopen(vfile, "w");
	fprintf(tmp, "%u %u %u %u %u %25.18lg\n", nRatings, nUsers, nItems, nValidations, nTests, mu);
	for(int i = 0 ; i < nUsers; i++ ) {
		fprintf(tmp, "%d\n", users[i].id);
	}
	for(int i = 0 ; i < nItems; i++ ) {
		fprintf(tmp, "%d\n", items[i].id);
	}
	fclose(tmp);
}

bool make_tmp_dir()
{
	struct stat st;
	if(stat(tmpdir, &st) == 0) {
		return false;
	} else {
		mkdir(tmpdir, 0777);
		return true;
	}
}


void read_stats()
{
	FILE *fp = fopen("stats1.txt", "r");
	char buf[512], *name, *value;
	//char *buf = tmp;
	unsigned long int d;
	while(fgets(buf, 512, fp) != NULL) {
		name = strtok(buf, "=");
		value = strtok(NULL, "=");
		d = strtoul(value, NULL, 10);
		if(strncmp(name, "nUsers", 512) == 0) {
			nUsers = d;
		} else if(strncmp(name, "nItems", 512) == 0) {
			nItems = d;
		} else if(strncmp(name, "nRatings", 512) == 0) {
			//pass
		} else if(strncmp(name, "nTrainRatings", 512) == 0) {
			nRatings = d;
		} else if(strncmp(name, "nProbeRatings", 512) == 0) {
			nValidations = d;
		} else if(strncmp(name, "nTestRatings", 512) == 0) {
			nTests = d;
		} else if(strncmp(name, "nGenres", 512) == 0) {
			nGenres = d;
		} else if(strncmp(name, "nAlbums", 512) == 0) {
			nAlbums = d;
		} else if(strncmp(name, "nArtists", 512) == 0) {
			nArtists = d;
		} else {
			fprintf(stderr, "ERROR unknown value in stats1.txt\n");
			exit(1);
		}
		fprintf(stdout, "%s=%lu\n", name, d);
	}

	fclose(fp);
}

void read_albumData()
{
	FILE *fp = fopen("albumData1.txt","r");
	int value;
	char *cur;
	char buf[200];
	int trackid, albumid, artistid;
	int aid = 0;
	while(! feof(fp) ) {
		fgets(buf, 200, fp);
		if (feof(fp)) break;

		albumid = atoi(strtok(buf, "|"));
		albumMap[albumid] = aid;

		if (imap.count(albumid) != 0) {
			int iid = imap[albumid];
			cur = strtok(NULL, "|");

			items[iid].albumid = aid;
			if (cur[0] != 'N') {
				items[iid].artistid = artistMap[atoi(cur)];
			}
			while( (cur = strtok(NULL, "|")) != NULL) {
				int value = atoi(cur);
				int gid = genreMap[value];
				genreItemMap.insert(hash_multimap<int,int>::value_type(iid, gid));
			}
		}
		aid++;
	}
}

void read_trackData()
{
	FILE *fp = fopen("trackData1.txt","r");
	int value;
	char *cur;
	char buf[200];
	int trackid, albumid, artistid;
	while(! feof(fp) ) {
		fgets(buf, 200, fp);
		if (feof(fp)) break;

		trackid = atoi(strtok(buf, "|"));

		if(imap.count(trackid) == 0 )
			continue;
		int iid = imap[trackid];
		items[iid].type = TRACK;
		cur = strtok(NULL, "|");
		if (cur[0] != 'N') {
			albumid = atoi(cur);
			items[iid].albumid = albumMap[albumid];
		}
		cur = strtok(NULL, "|");
		if (cur[0] != 'N') {
			artistid = atoi(cur);
			items[iid].artistid = artistMap[artistid];
		}
		while( (cur = strtok(NULL, "|")) != NULL) {
			int value = atoi(cur);
			int gid = genreMap[value];
			genreItemMap.insert(hash_multimap<int,int>::value_type(iid, gid));
		}
	}
}

void read_genres()
{
	FILE *fp = fopen("genreData1.txt","r");
	int lines = 0, value;
	int gid = 0;
	while(! feof(fp) ) {
		fscanf(fp, "%d\n", &value);
		if(feof(fp)) break;
		genreMap[value] = gid;
		if(imap.count(value) > 0) {
			int iid = imap[value];
			items[iid].type = GENRE;
			genreItemMap.insert(hash_multimap<int,int>::value_type(iid, gid));
		}
		gid++;
	}
}

void read_artists()
{
	FILE *fp = fopen("artistData1.txt","r");
	int lines = 0, value;
	int aid = 0;
	while(! feof(fp) ) {
		fscanf(fp, "%d\n", &value);
		if(feof(fp)) break;
		artistMap[value] = aid;
		if(imap.count(value) != 0) {
			int iid = imap[value];
			items[iid].type = ARTIST;
			items[iid].artistid = aid;
			items[iid].albumid = -1;
		}
		aid++;
	}
}

void load_ratings() {
	ratings = (struct rating_s*)open_rw("ratings.mmap", sizeof(rating_s)*nRatings);
	validations= (struct rating_s*)open_rw("validations.mmap", sizeof(rating_s)*nValidations);
	tests = (struct rating_s*)open_rw("tests.mmap", sizeof(rating_s)*nTests);
	items = (struct item_s*)open_rw("items.mmap", sizeof(item_s)*nItems, MADV_RANDOM);
	users = (struct user_s*)open_rw("users.mmap", sizeof(user_s)*nUsers);
}

void load_data(bool force_reload /*= false*/)
{
	bool isNew = make_tmp_dir();
	read_stats();

	if(isNew || force_reload) {
		load_ratings();
		read_data();
	} else {
		load_ratings();
		char vfile[256];
		strcpy(vfile, tmpdir);
		strcat(vfile,"values");
		FILE *tmp = fopen(vfile, "r");
		fscanf(tmp, "%u %u %u %u %u %lg\n", &nRatings, &nUsers, &nItems, &nValidations, &nTests, &mu);
		int lk = 0;
		for(int i = 0 ; i < nUsers; i++ ) {
			fscanf(tmp, "%d\n", &lk);
			umap[lk] = i;
		}
		for(int i = 0 ; i < nItems; i++ ) {
			fscanf(tmp, "%d\n", &lk);
			imap[lk] = i;
		}
		fclose(tmp);
	}
	read_genres();
	read_artists();
	read_albumData();
	read_trackData();
}

void set_tmp_dir(const char *dir)
{
	int n = strlen(dir);
	strcpy(tmpdir, dir);
	if (dir[n-1] != '/') {
		tmpdir[n] = '/';
		tmpdir[n+1] = '\0';
	}
	return;
}

/*FILE *save_whole_results() {
	FILE *file *fopen("model");
	for(int u = 0; u < nUsers; u++) {
		for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
			struct rating_s rating = ratings[r];
			printf("%lg\n", predict(u, rating.item, rating.day));
		}
	}
	for(int u = 0; u < nUsers; u++) {
		for(int r = u*4; r < (u+1)*4; r++) {
			struct rating_s rating = validaions[r];
			printf("%lg\n", predict(u, rating.item, rating.day));
		}
	}
	for(int u = 0; u < nUsers; u++) {
		for(int r = u*6; r < (u+1)*6; r++) {
			struct rating_s rating = tests[r];
			printf("%lg\n", predict(u, rating.item, rating.day));
		}
	}
	fclose(file);
	return NULL;
}*/
