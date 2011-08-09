#ifndef DATA_H
#define DATA_H

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

using namespace std;
using namespace __gnu_cxx;

#define SCORENORM 1.

#define BIASES 1
#define BIASESG 1
#define BIASESA 1
#define LATENT 1
#define LATENTAL 1
#define LATENTAR 1
#define LATENTG 1

#define NBINIT 70
#define ITBIN(x) ( x / 57)

using namespace std;
using namespace __gnu_cxx;

extern char tmpdir[256];

extern unsigned int nUsers;
extern unsigned int nItems;
extern unsigned int nRatings;
extern unsigned int nValidations;
extern unsigned int nTests;
extern unsigned int nGenres;
extern unsigned int nAlbums;
extern unsigned int nArtists;


#define STARTRATING(u) users[u].start
#define ENDRATING(u) users[u].start + users[u].count
#define MIN(a,b) (((a) < (b)) ? (a) : (b))
#define MAX(a,b) (((a) > (b)) ? (a) : (b))
#define CLAMP(p) MAX(MIN(p,100./SCORENORM), 0.)
#define CLAMP2(p) MAX(MIN(p,100./SCORENORM), -100./SCORENORM)

enum {
	NONE = 0,
	TRACK,
	ALBUM,
	ARTIST,
	GENRE,
};

struct rating_s {
	//unsigned int user : 20;
	unsigned int item : 20;
	unsigned int day : 12;
	float rating;
	//float extra;
	//unsigned int rating : 8;
       	//signed int extra : 8;
}; // __attribute__((__packed__));

struct item_s {
	uint32_t id;
	uint32_t count;
	unsigned int type : 3;
	int artistid : 21;
	int albumid : 21;
};

struct user_s {
	uint32_t id;
	uint32_t count;
	uint32_t start;
};


typedef hash_multimap<int, int> multimapII;
extern hash_map<int, int> genreMap;
extern hash_map<int, int> artistMap;
extern hash_map<int, int> albumMap;
extern hash_multimap<int, int> genreItemMap;
extern unsigned int umaplen, imaplen;
extern hash_map<int, int> imap;
extern hash_map<int, int> umap;
extern struct rating_s *ratings;
extern struct item_s *items;
extern struct user_s *users;
extern struct rating_s *validations;
extern struct rating_s *tests;
extern double mu;

void *open_read(const char *file, ssize_t *size);
void *open_rw(const char *file1, ssize_t size, int advice = MADV_SEQUENTIAL);

inline int parseInt(char *s, ssize_t *i, int *a, ssize_t size) ;
void read_tests();
void read_validate();
void read_data();
void read_stats();
void read_albumData();
void read_trackData();
void read_genres();

void read_artists();
bool make_tmp_dir();

void load_data(bool force_reload = false);
void set_tmp_dir(const char *dir);

#endif// DATA_H
