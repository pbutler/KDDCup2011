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
#include "barrier.h"
#include <sys/time.h>
#include <assert.h>

#define BIASES 1
#define BIASESG 1
#define BIASESA 1
#define LATENT 1
#define LATENTA 1


using namespace std;
using namespace __gnu_cxx;

//**START PARAMS
double artistStep = 0.0550822;
double itemReg = 0.00991796;
double itemStep = 0.0133056;
double decay = 0.499984;
double artistReg = 2.22105;
double genreReg = 2.87483;
double userReg = 0.131461;
double userStep = 1.1487;
double albumStep = 0.000112683;
double albumReg = 1.29896;
double genreStep = 0.155072;
//**END PARAMS


double pStep = 0.0204848;
double decaypq = 0.734298;
double qStep = 0.000876522;
double albumStep2 = 0.0327189434601;
double pReg = 0.560498915047;
double qReg = 2.08076132658;
double decay2 = 0.682039876166;
double itemStep2 = 0.0125042784087;
double artistStep2 = 0.0133653874299;
double userStep2 = 0.517686752997;

double xStep		= 1e-6;
double xReg		= .05;
double yStep		= 1e-6;
double yReg		= .05 ;

double pMax = 3;
double qMax = 3;
double xMax = .1;
double yMax = .1;

double pMin = -1 * pMax;
double qMin = -1 * qMax;
double xMin = -1 * xMax;
double yMin = -1 * yMax;


double pArStep = 0.005;
double pArReg = .01;

double pAlStep = 0.005;
double pAlReg = .01;

#define NUM_THREADS 1
#define SCORENORM  1.f

int maxepochs = 20;
int maxepochsbias = 20;
int maxfaults = 3;

unsigned int nUsers = 0;
unsigned int nItems = 0;
unsigned int nRatings = 0;
unsigned int nValidations =0;
unsigned int nTests = 0;
unsigned int nGenres = 0;
unsigned int nAlbums = 0;
unsigned int nArtists = 0;

const unsigned int nFeatures = 100;

pthread_mutex_t mutexB = PTHREAD_MUTEX_INITIALIZER;
int curItems[NUM_THREADS];
pthread_mutex_t mutexFeature[nFeatures];

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
	//float rating;
	//float extra;
	unsigned int rating : 8;
       	//signed int extra : 8;
}; // __attribute__((__packed__));
//};

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


double terrs[NUM_THREADS];

typedef hash_multimap<int, int> multimapII;
hash_map<int, int> genreMap;
hash_map<int, int> artistMap;
hash_map<int, int> albumMap;
hash_multimap<int, int> genreItemMap;
unsigned int umaplen = 0,imaplen = 0;
hash_map<int, int> imap;
hash_map<int, int> umap;
double *bu, *bi, *p, *x, *y, *q, *bg, *bal, *bar;
double *pAl, *pAr, *qAl, *qAr;
struct rating_s *ratings;
struct item_s *items;
struct user_s *users;
struct rating_s *validations;
struct rating_s *tests;
double mu;

pthread_barrier_t barrier;

inline double collect_values(double err, int rank) {
	terrs[rank] = err;
	pthread_barrier_wait(&barrier);
	err = 0;
	for(int i = 0; i < NUM_THREADS; i++) {
		err += terrs[0];
	}
	return err;
}

double get_time()
{
    struct timeval t;
    struct timezone tzp;
    gettimeofday(&t, &tzp);
    return t.tv_sec + t.tv_usec*1e-6;
}

inline double randF(double min, double max)
{
   double a =  (double)random() /((double)RAND_MAX +1);
   return a * (max - min) + min;
}

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
	return data;
}

void *open_rw(const char *file, ssize_t size, int advice = MADV_SEQUENTIAL)
{
	struct stat sbuf;
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


void load_model()
{
	bal = (double*) open_rw("tmp/bal.mmap", sizeof(double)*nAlbums);
	bar = (double*) open_rw("tmp/bar.mmap", sizeof(double)*nArtists);
	bg = (double*) open_rw("tmp/bg.mmap", sizeof(double)*nGenres);
	bi = (double*) open_rw("tmp/bi.mmap", sizeof(double)*nItems);
	bu = (double*)open_rw("tmp/bu.mmap", sizeof(double)*nUsers);
	ratings = (struct rating_s*)open_rw("tmp/ratings.mmap", sizeof(rating_s)*nRatings);
	validations= (struct rating_s*)open_rw("tmp/validations.mmap", sizeof(rating_s)*nValidations);
	tests = (struct rating_s*)open_rw("tmp/tests.mmap", sizeof(rating_s)*nTests);
	items = (struct item_s*)open_rw("tmp/items.mmap", sizeof(item_s)*nItems, MADV_RANDOM);
	users = (struct user_s*)open_rw("tmp/users.mmap", sizeof(user_s)*nUsers);

	p = (double *)open_rw("tmp/p.mmap", sizeof(double)*nFeatures*nUsers);
	q = (double*)open_rw("tmp/q.mmap", sizeof(double)*nFeatures*nItems, MADV_RANDOM);
	x = (double*)open_rw("tmp/x.mmap", sizeof(double)*nFeatures*nItems, MADV_RANDOM);
	y = (double*)open_rw("tmp/y.mmap", sizeof(double)*nFeatures*nItems, MADV_RANDOM);

	pAl = (double *)open_rw("tmp/pAl.mmap", sizeof(double)*nFeatures*nUsers);
	qAl = (double*)open_rw("tmp/qAl.mmap", sizeof(double)*nFeatures*nAlbums, MADV_RANDOM);
	pAr = (double *)open_rw("tmp/pAr.mmap", sizeof(double)*nFeatures*nUsers);
	qAr = (double*)open_rw("tmp/qAr.mmap", sizeof(double)*nFeatures*nArtists, MADV_RANDOM);
}

void *init_model(void *ptr = NULL) {
	int rank = 0;
	if(ptr != NULL)  {
		rank = *(int *)ptr;
	}
	for(int i =rank; i < nUsers; i += NUM_THREADS) {
	       bu[i] = 0;
	}
	for(int i = rank; i < nItems; i+=NUM_THREADS) {
	       bi[i] = 0;
	}
	for(int i = rank; i < nGenres; i+= NUM_THREADS) {
		bg[i] = 0;
	}
	for(int i =rank; i < nArtists; i += NUM_THREADS) {
	       bar[i] = 0;
	}
	for(int i =rank; i < nAlbums; i += NUM_THREADS) {
	       bal[i] = 0;
	}
	double sq, err=1e6, pred, lasterr = 1e6+1, vBest= 1e6;
	int epoch = 0, faults = 0;
	while (epoch < maxepochsbias && faults < maxfaults) {
		double start = get_time();
		sq = 0;
		for(int u = rank; u < nUsers; u+=NUM_THREADS) {
			for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
				struct rating_s rating = ratings[r];
				struct item_s item = items[rating.item];
				double tmpbi = bi[rating.item];
#ifdef BIASES
				pred = mu + bu[u] + tmpbi;
#else
				pred = 0;
#endif
				//fprintf(stderr, "%d\n", rating.item);
#ifdef BIASESG
				pair<multimapII::const_iterator, multimapII::const_iterator> p =
					genreItemMap.equal_range(rating.item);
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					pred += bg[(*i).second];
				}
#endif
#ifdef BIASESA
				if(item.artistid > -1) {
					pred += bar[item.artistid];
				}
				if(item.albumid > -1) {
					pred += bal[item.albumid];
				}
#endif
				pred = CLAMP(pred);
				err = (double)rating.rating - pred;
				sq += err*err*SCORENORM*SCORENORM;
#ifdef BIASES
				bu[u] += userStep*(err - userReg*bu[u]);
				bi[rating.item] += itemStep*(err - itemReg*tmpbi);
#endif // BIASES
#ifdef BIASESG
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					int gid = (*i).second;
					bg[gid] += genreStep*(err - genreReg*bg[gid]);
				}
#endif //BIASESG
#ifdef BIASESA
				if(item.artistid > -1) {
					bar[item.artistid] += artistStep*(err - artistReg*bar[item.artistid]);
				}
				if(item.albumid > -1) {
					bal[item.albumid] += albumStep*(err - albumReg*bal[item.albumid]);
				}
#endif //BIASESA
				/*pthread_mutex_lock(&mutexB);
				bi[rating.item] += itemStep2*(err - itemReg*tmpbi);
				pthread_mutex_unlock(&mutexB);
				*/
				//printf("%g %g %g\n", err, rating.rating, pred);
				//printf("%g\n", bi[ratings[r].item]);

			}
		}
		sq = collect_values(sq, rank);
		sq = sqrt(sq / (double)nRatings);
		double vsq = 0;
		for(int u = rank; u < nUsers; u+=NUM_THREADS) {
			for(int r = u*4; r < (u+1)*4; r++) {
				struct rating_s rating = validations[r];
				struct item_s item = items[ratings[r].item];
#ifdef BIASES
				pred = mu + bu[u] + bi[rating.item];
#else
				pred = 0;
#endif //BIASES
				pair<multimapII::const_iterator, multimapII::const_iterator> p =
					genreItemMap.equal_range(rating.item);
				//printf("%g %g %g %d ", mu, bu[u], bi[rating.item],rating.item);
#ifdef BIASESG
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					int gid = (*i).second;
					pred += bg[gid];
				}
#endif //BIASESG
#ifdef BIASESA
				if(item.artistid > -1) {
					pred += bar[item.artistid];
				}
				if(item.albumid > -1) {
					pred += bal[item.albumid];
				}
#endif // BIASESA
				//printf("\n");
				pred = CLAMP(pred);
				err = (double)rating.rating - pred;
				vsq += err*err*SCORENORM*SCORENORM;
			}
			//printf("%g %d %g\n", err, validations[i].rating, pred);
		}

		vsq = collect_values(vsq, rank);
		vsq = sqrt(vsq / (double)nValidations);
		err = vsq;
		vBest = MIN(vsq, vBest);

		if (rank == 0 ) {
			printf("BIAS epoch=%d RMSE=%g VRMSE=%g time=%g\n", epoch, sq, vsq, get_time()-start, sq);
		}
		if (err > lasterr) {
			faults++;
		} else {
			artistStep *= decay;
			albumStep *= decay;
			genreStep *= decay;
			itemStep *= decay;
			userStep *= decay;
			faults = 0;
		}
		epoch++;
		lasterr = err;
	}

	if (rank == 0)
		printf("Best is %lf\n", vBest);
#if defined(LATENT) || defined(LATENTA)
	double sqnF = 1./sqrt(nFeatures);
	for(int i = rank; i < nUsers*nFeatures;i += NUM_THREADS) {
		p[i] = randF(pMin, pMax) * sqnF;
		pAl[i] = randF(pMin, pMax) * sqnF;
		pAr[i] = randF(pMin, pMax) * sqnF;
	}

	for(int i = rank; i < nAlbums*nFeatures;i+=NUM_THREADS) {
		qAl[i] = randF(qMin, qMax) * sqnF;
	}
	for(int i = rank; i < nArtists*nFeatures;i+=NUM_THREADS) {
		qAr[i] = randF(qMin, qMax) * sqnF;
	}
	for(int i = rank; i < nItems*nFeatures;i+=NUM_THREADS) {
		q[i] = randF(qMin, qMax) * sqnF;
		x[i] = randF(xMin, xMax) * sqnF;
		y[i] = randF(yMin, yMax) * sqnF;
	}
#endif
	pthread_barrier_wait(&barrier);
#if 0
	double sum = 0;
	for(int u = rank; u < nUsers; u += NUM_THREADS) {
		for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
			struct item_s item = items[ratings[r].item];
#ifdef BIASES
			double allbias = (double)ratings[r].rating - mu -bu[u] - bi[ratings[r].item];
#else
			double allbias = (double)ratings[r].rating ;
#endif //BIASES
#ifdef BIASESG
			pair<multimapII::const_iterator, multimapII::const_iterator> p = genreItemMap.equal_range(ratings[r].item);
			for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
				int gid = (*i).second;
				allbias -= bg[gid];
			}
#endif //BIASESG
#ifdef BIASESA
			if(item.artistid > -1) {
				allbias -= bar[item.artistid];
			}
			if(item.albumid > -1) {
				allbias -= bal[item.albumid];
			}
#endif //BIASESA
			//ratings[r].extra = (int)CLAMP(ratings[r].extra);

			ratings[r].extra = (int)allbias;
			sum += allbias;
		}
	}
	sum = collect_values(sum, rank);
	if (rank == 0 ) {
		printf("mu = %g\n", mu);
		printf("Average deviation: %g\n", sum/nRatings);
	}
#endif
	return 0;
}

inline double predict(int uid, int iid, bool print = false)
{
	struct item_s item = items[iid];
	double sum = 0;
	if (print)
		printf("\n\n");
#ifdef BIASES
	sum += mu + bu[uid] + bi[iid];
	if(print)
		printf("bu/i: %g %g %g\n", mu, bu[uid], bi[iid]);
#endif //BIASES
#ifdef BIASESG
	bool goprint = false;
	pair<multimapII::const_iterator, multimapII::const_iterator> gp =
		genreItemMap.equal_range(iid);
	for (multimapII::const_iterator i = gp.first; i != gp.second; ++i) {
		int gid = (*i).second;
		if (!goprint && print) {
			printf("bg: ");
		}
		goprint = true;
		sum += bg[gid];
		if(print)
			printf("%g ", bg[gid]);
	}
	if(print && goprint)
		printf("\n");
#endif //BIASESG
	double apq = 0;
#ifdef LATENT
	for(int f = 0; f < nFeatures; f++) {
		if (print )
			printf("pq: %g %g %g\n",
				       	p[uid*nFeatures+f],
					q[iid*nFeatures+f],
					q[iid*nFeatures+f] * p[uid*nFeatures+f]);
		apq += CLAMP2(q[iid*nFeatures+f] * p[uid*nFeatures+f]);
	}
	sum += apq;
	if(print)
		printf("pqT: %g\n", apq);
#endif //LATENT
#ifdef LATENTA
	apq = 0;
	if(item.albumid > -1) {
		sum += bal[item.albumid];
		for(int f = 0; f < nFeatures; f++) {
			apq += CLAMP2(qAl[item.albumid*nFeatures+f] * pAl[uid*nFeatures+f]);
		}
	}
	sum += apq;
	if(print)
		printf("Alpq: %g\n", apq);
	apq = 0;
	if(item.artistid > -1) {
		sum += bar[item.artistid];
		for(int f = 0; f < nFeatures; f++) {
			apq += CLAMP2(qAr[item.artistid*nFeatures+f] * pAr[uid*nFeatures+f]);
		}
	}
	sum += apq;
	if(print)
		printf("Arpq: %g\n", apq);
#endif //LATENT
	if(print)
		printf("%g --> ", sum);
	return CLAMP(sum);
}

double validate(bool print = false)
{
	double sq = 0;
	for(int u = 0; u < nUsers; u++) {
		for(int r = u*4; r < (u+1)*4; r++) {
			double pred = predict(u, validations[r].item, print);
			double err = (double)validations[r].rating - pred;
			sq += err*err*SCORENORM*SCORENORM;
			if (print) {
				printf("%d %d %g %g %g\n", u, validations[r].item, validations[r].rating*SCORENORM, pred*SCORENORM, err*SCORENORM);
			}
		}
	}
	return sqrt(sq/nValidations);

}

void make_predictions()
{
	FILE *fp = fopen("predictions.txt", "w");
	for(int u = 0; u < nUsers; u++) {
		for(int r = 0; r < 6; r++) {
			int iid = u*6 +r;
			double pred = predict(u,tests[iid].item);
			fprintf(fp, "%lf\n", pred); //*SCORENORM);
		}
	}
	fclose(fp);
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
			score = (double) stmp / SCORENORM;
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
			ridx++;
		}
	}
	munmap(data, size);
	nValidations = ridx;
	printf("nValidations = %d\n", nValidations);
}

void *train_model(void *ptr = NULL) {
	int rank = 0;
	double sum[nFeatures];
	if(ptr != NULL)  {
		rank = *(int *)ptr;
	}
	double lasterr = 1e6+1, sq = 0, vBest = 1e6;
	int epoch = 0, faults = 0;
	while (faults <  maxfaults && epoch < maxepochs) {
	//while (epoch < maxepochs) { // && (epochs < 10 || (lasterr - err) > 1e-6))
		sq = 0;
		double start = get_time();

		pthread_barrier_wait(&barrier);
		for(int u = rank; u < nUsers; u += NUM_THREADS) {
			struct user_s user = users[u];
			double invsqru = sqrt( 1. / (double)user.count);
			double invsqnu = sqrt( 1. / (double)(user.count +10)); //user.count+user.qu));
			int uf =  u*nFeatures;
			/*for(int f = 0; f < nFeatures; f++) {
				p[uf + f] = 0;

				for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
					float rb = ratings[r].extra;
					int j = (ratings[r].item*nFeatures);
					p[uf+f] += rb * x[j+f] * invsqru;
					p[uf+f] += y[j+f] * invsqnu;
				}
				for(int r = u*4; r < (u+1)*4; r++) {
					int j = (validations[r].item*nFeatures);
					p[uf+f] += y[j+f] * invsqnu;
				}
				for(int r = u*6; r < (u+1)*6; r++) {
					int j = (tests[r].item*nFeatures);
					p[uf+f] += y[j+f] * invsqnu;
				}
				sum[f] = 0.;
			}*/

			//pthread_barrier_wait(&barrier);
			for(int r = STARTRATING(u); r < ENDRATING(u); r++ ) {
				struct rating_s &rating = ratings[r];
				struct item_s &item = items[rating.item];
				double err = (double) rating.rating - predict(u, rating.item);
				double tmpbi = bi[rating.item];
				if (err > 100 || err < -100) {
					printf("ERR OOB: %g %g %g\n", err, (double)rating.rating, predict(u, rating.item));
				}
				sq += err*err*SCORENORM*SCORENORM;
#ifdef LATENT
				for(int f = 0; f < nFeatures; f++) {
					double tmpq = q[rating.item*nFeatures+f];
					double tmpp = p[uf + f];
					//sum[f] += err*tmpq;
					q[rating.item*nFeatures+f] += qStep*(err*tmpp - qReg*tmpq);
					p[uf+f] += pStep*(err*tmpq - pReg*tmpp);
				}
#endif //LATENT
#ifdef BIASES
				bu[u] += userStep2*(err - userReg*bu[u]);
				bi[rating.item] += itemStep2*(err - itemReg*tmpbi);
#endif //BIASES

				if(item.albumid > -1) {
#ifdef BIASA
					bal[item.albumid] += albumStep2*(err - albumReg*bal[item.albumid]);
#endif //BIASA
#ifdef LATENTA
					for(int f = 0; f < nFeatures; f++) {
						int iif = item.albumid*nFeatures+f;
						double tmpq = qAl[iif];
						double tmpp = pAl[uf+f];
						qAl[iif] += qStep*(err*tmpp - qReg*tmpq);
						pAl[uf+f] += pStep*(err*tmpq - pReg*tmpp);
					}
#endif // LATENTA
				}
				if(item.artistid > -1) {
#ifdef BIASA
					bar[item.artistid] += artistStep2*(err - artistReg*bar[item.artistid]);
#endif //BIASA
#ifdef LATENTA
					for(int f = 0; f < nFeatures; f++) {
						int iif = item.artistid*nFeatures+f;
						double tmpq = qAr[iif];
						double tmpp = pAr[uf+f];
						qAr[iif] += qStep*(err*tmpp - qReg*tmpq);
						pAr[uf+f] += pStep*(err*tmpq - pReg*tmpp);
					}
#endif //LATENTA
				}
				//pthread_mutex_unlock(&mutexB);
			}


			/*for(int f = 0; f < nFeatures; f++) {
				pthread_mutex_lock(&mutexFeature[f]);
				for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
					struct rating_s rating = ratings[r] ;
					float rb = ratings[r].extra;
					int i = rating.item*nFeatures + f;
					x[i] += xStep*(invsqru*rb*sum[f] - xReg*x[i]);
					y[i] += yStep*(invsqnu*sum[f] - yReg*y[i]);
				}
				for(int r = 4*u; r < 4*(u+1); r++) {
					struct rating_s rating = validations[r];
					int i = rating.item*nFeatures + f;
					y[i] += yStep*(invsqnu*sum[f] - yReg*y[i]);
				}
				for(int r = 6*u; r < 6*(u+1); r++) {
					struct rating_s rating = tests[r];
					int i = rating.item*nFeatures + f;
					y[i] += yStep*(invsqnu*sum[f] - yReg*y[i]);
				}
				pthread_mutex_unlock(&mutexFeature[f]);
			}*/
		}
		sq = collect_values(sq, rank);
		sq = sqrt(sq / nRatings);
		double verr = validate();
		vBest = min(verr, vBest);

		if (rank == 0) {
			printf("epoch=%d RMSE=%g VRMSE=%g" , epoch, sq, verr);
			printf(" time=%g rank=%d\n", get_time()-start, rank);

		}
		epoch++;
		if (verr > lasterr) {
			faults++;
		} else {
			pStep *= decaypq;
			qStep *= decaypq;
			xStep *= decay;
			yStep *= decay;
			userStep2 *= decay2;
			itemStep2 *= decay2;
			faults = 0;
		}
		lasterr = verr;
	}
	if (rank == 0)
		printf("Best is %lf\n", vBest);
	return 0;
}

#if 0
void kickoff(void *(*foo)(void*))
{
	foo((void *)0);
}
#endif
#if 1
void kickoff(void *(*foo)(void*))
{
	pthread_t thread[NUM_THREADS];
	int ranks[NUM_THREADS];
	pthread_attr_t attr;
	int rc;
	void *status;

	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	for(int t=0; t<NUM_THREADS; t++) {
		printf("Main: creating thread %d\n", t);
		ranks[t] = t;
		rc = pthread_create(&thread[t], &attr, foo, (void*)(ranks+t));
		if (rc) {
			printf("ERROR; return code from pthread_create() is %d\n", rc);
			exit(-1);
		}
	}

	/* Free attribute and wait for the other threads */
	pthread_attr_destroy(&attr);
	for(int t=0; t<NUM_THREADS; t++) {
		rc = pthread_join(thread[t], &status);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
		printf("Main: completed thread %d having a status of %ld\n",t,(long)status);
	}
}
#endif

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
			//score /= SCORENORM;
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

	FILE *tmp = fopen("tmp/values", "w");
	fprintf(tmp, "%u %u %u %u %u %25.18lg\n", nRatings, nUsers, nItems, nValidations, nTests, mu);
	for(int i = 0 ; i < nUsers; i++ ) {
		fprintf(tmp, "%d\n", users[i].id);
	}
	for(int i = 0 ; i < nItems; i++ ) {
		fprintf(tmp, "%d\n", items[i].id);
	}
	fclose(tmp);
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

		if(imap.count(albumid) == 0 )
			continue;
		int iid = imap[albumid];
		cur = strtok(NULL, "|");

		items[iid].albumid = aid;
		if (cur[0] == 'N') {
			items[iid].artistid = -1;
		} else {
			items[iid].artistid = artistMap[atoi(cur)];
		}
		while( (cur = strtok(NULL, "|")) != NULL) {
			int value = atoi(cur);
			int gid = genreMap[value];
			genreItemMap.insert(hash_multimap<int,int>::value_type(iid, gid));
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
		if (cur[0] == 'N') {
			items[iid].albumid = -1;
		} else {
			albumid = atoi(cur);
			items[iid].albumid = albumMap[albumid];
		}
		cur = strtok(NULL, "|");
		if (cur[0] == 'N') {
			items[iid].artistid = -1;
		} else {
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
		if(imap.count(value) == 0)
			continue;
		int iid = imap[value];
		items[iid].type = ARTIST;
		artistMap[value] = aid;
		items[iid].artistid = aid;
		items[iid].albumid = -1;
		aid++;
	}
}

void print_model_stats()
{
	FILE *fp = fopen("modelstats.txt", "w");
	fprintf(fp, "# --------------\n");
	fprintf(fp, "# Stats\n");
	fprintf(fp, "# --------------");

	for(int f = 0; f < nFeatures; f++) {
		fprintf(fp, "\n\n\n#p%03d\n", f);
		for(int  i = 0; i < nUsers; i++) {
			//fprintf(fp, "%g\n", p[i*nFeatures + f]);
		}
		fprintf(fp, "\n\n\n#q%03d\n", f);
		for(int  i = 0; i < nItems; i++) {
			fprintf(fp, "%g\n", q[i*nFeatures + f]);
		}
		fprintf(fp, "\n\n\n#x%03d\n", f);
		for(int  i = 0; i < nItems; i++) {
			fprintf(fp, "%g\n", x[i*nFeatures + f]);
		}
		fprintf(fp, "\n\n\n#y%03d\n", f);
		for(int  i = 0; i < nItems; i++) {
			fprintf(fp, "%g\n", y[i*nFeatures + f]);
		}
	}
	fprintf(fp, "\n\n\n# MEAN(global) = %g\n\n\n", mu);
	fprintf(fp, "# User Biases\n");
	double a = 0;
	for(int i = 0; i < nUsers; i++) {
		fprintf(fp, "%g\n", bu[i]);
		a += bu[i];
	}
	fprintf(fp, "# MEAN(bu) = %g\n\n\n", a/(double)nUsers);

	a = 0;
	fprintf(fp, "# Item Biases\n");
	for(int i = 0; i < nItems; i++) {
		fprintf(fp, "%g\n", bi[i]);
		a += bi[i];
	}
	fprintf(fp, "# MEAN(bi) = %g\n\n\n", a/(double)nItems);

	a = 0;
	fprintf(fp, "# Genre Biases\n");
	for(int i = 0; i < nGenres; i++) {
		fprintf(fp, "%g\n", bg[i]);
		a += bg[i];
	}
	fprintf(fp, "# MEAN(bg) = %g\n\n\n", a/(double)nGenres);

	a = 0;
	fprintf(fp, "# Album Biases\n");
	for(int i = 0; i < nAlbums; i++) {
		fprintf(fp, "%g\n", bal[i]);
		a += bal[i];
	}
	fprintf(fp, "# MEAN(bal) = %g\n\n\n", a/(double)nAlbums);

	a = 0;
	fprintf(fp, "# Artist Biases\n");
	for(int i = 0; i < nArtists; i++) {
		fprintf(fp, "%g\n", bar[i]);
		a += bar[i];
	}
	fprintf(fp, "# MEAN(bar) = %g\n\n\n", a/(double)nArtists);
	fprintf(fp, "# --------------\n");
}

void make_tmp_dir()
{
	struct stat st;
	if(stat("tmp",&st) == 0)
		return;
	else
		mkdir("tmp", 0777);
}

void read_params(void)
{
	FILE *fp = fopen("params.txt", "r");
	char buf[512], *name, *value;
	//char *buf = tmp;
	double d;
	while(fgets(buf, 512, fp) != NULL) {
		name = strtok(buf, "=");
		value = strtok(NULL, "=");
		bool set = false;
#define READSET(x) if(strncmp(name, #x, 512) == 0) { x = d; set=true; printf("%s=%g",name,x); }
		d = strtod(value, NULL);
		READSET(decay);
		READSET(itemStep);
		READSET(itemReg);
		READSET(userStep);
		READSET(userReg);
		READSET(genreStep);
		READSET(genreReg);
		READSET(albumStep);
		READSET(albumReg);
		READSET(artistStep);
		READSET(artistReg);

		READSET(userStep2);
		READSET(itemStep2);
		READSET(pStep);
		READSET(pReg);
		READSET(qStep);
		READSET(qReg);
		READSET(xStep);
		READSET(xReg);
		READSET(yStep);
		READSET(yReg);
		READSET(qMin);
		READSET(xMin);
		READSET(yMin);
		READSET(qMax);
		READSET(xMax);
		READSET(yMax);

		READSET(decaypq);
		READSET(itemStep2 );
		READSET(userStep2);
		READSET(artistStep2);
		READSET(albumStep2);
		READSET(decay2);
		if (!set) {
			fprintf(stdout, "OOPS! param notfound %s\n", name);
		} else {
			fprintf(stdout, "%s=%g\n", name, d);
		}
	}

	fclose(fp);
}


void print_usage(char *progname)
{
	printf("usage: %s [OPTIONS]\n", progname);
	printf("\n\n");
	printf("    -i\t\tInit data files and build binary files\n");
	printf("    -m\t\tInitialize models only\n");
	printf("    -t\t\tTrain model\n");
	printf("    -v\t\tPrint validation data\n");
	printf("    -p\t\tGenerate prediction file on test data\n");
	printf("    -s\t\tPrint model stats\n");
	printf("    -f <n>\tmax faults before stopping training\n");
	printf("    -E <n>\tmax epochs in baseline training\n");
	printf("    -e <n>\tmax epochs in full model training\n");
	printf("\n\n");
}

int main (int argc, char **argv)
{
	//srandom(time(NULL));
	int c;
	bool isInit = false;
	bool isTrain = false;
	bool isVerify = false;
	bool isBuildModel = false;
	bool isStats = false;
	bool isPredict = false;
	bool printVerify = true;

	while ( (c = getopt(argc, argv, "itvVme:spf:bE:hP")) != -1) {
		switch (c) {
			case 'i':
				isInit = true;
				break;
			case 'm':
				isBuildModel = true;
				break;
			case 't':
				isTrain = true;
				break;
			case 'v':
				isVerify = true;
				break;
			case 'V':
				isVerify = true;
				printVerify = false;
				break;
			case 's':
				isStats = true;
				break;
			case 'e':
				maxepochs = atoi(optarg);
				break;
			case 'E':
				maxepochsbias = atoi(optarg);
				break;
			case 'f':
				maxfaults = atoi(optarg);
				break;
			case 'p':
				isPredict = true;
				break;
			case 'P':
				read_params();
				break;
			case 'h':
				print_usage(argv[0]);
				exit(0);
				break;
			case '?':
				break;
			default:
				printf ("?? getopt returned character code 0%o ??\n", c);
		}
	}
	if (optind < argc) {
		while (optind < argc)
			printf ("%s ", argv[optind++]);
		printf ("\n");
	}

	pthread_barrier_init(&barrier, NULL, NUM_THREADS);

	make_tmp_dir();

	read_stats();

	for(int i = 0; i < nFeatures; i++) {
		pthread_mutex_init(&mutexFeature[i], NULL);
	}

	if(!isInit) {
		FILE *tmp = fopen("tmp/values", "r");
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

	load_model();

	if(isInit) {
		read_data();
	}

	read_genres();
	read_artists();
	read_albumData();
	read_trackData();

	if (isInit || isBuildModel) {
		kickoff(init_model);
	}

	//print_model_stats();

	if(isTrain) {
		kickoff(train_model);
	}

	if (isStats) {
		print_model_stats();
	}

	if (isVerify) {
		printf("VRMSE = %g\n", validate(printVerify));
	}

	if(isPredict) {
		make_predictions();
	}
	pthread_barrier_destroy(&barrier);
	pthread_exit(NULL);

	exit (0);
}


