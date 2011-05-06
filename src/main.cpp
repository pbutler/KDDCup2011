#include <stdint.h>
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

using namespace std;
using namespace __gnu_cxx;

double itemStep		= 0.005;
double itemReg		= 1;
double userStep		= 1.5;
double userReg		= 1;
double genreStep	= 0.005;
double genreReg		= 1;

double GAMMA = .005; //.07;
double LAMBDA = 0.05;
double GAMMA2 = .005 / 100; // / 10000;//.0001;
double LAMBDA2 =  1;//0005; //100; //GAMMA2*40;

const double decay = .9;
#define NUM_THREADS 1
#define SCORENORM  100.f

int maxepochs = 20;
int maxepochsbias = 20;
int maxfaults = 2;

unsigned int nUsers = 0;
unsigned int nItems = 0;
unsigned int nRatings = 0;
unsigned int nValidations =0 ;
unsigned int nTests = 0;
unsigned int nGenres = 0;

const unsigned int nFeatures = 1;

pthread_mutex_t mutexB = PTHREAD_MUTEX_INITIALIZER;
int curItems[NUM_THREADS];
pthread_mutex_t mutexFeature[nFeatures];

#define STARTRATING(u) users[u].start
#define ENDRATING(u) users[u].start + users[u].count

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
	float rating;
	float extra;
};

struct item_s {
	uint32_t id;
	uint32_t count;
	unsigned int type : 3;
};

struct user_s {
	uint32_t id;
	uint32_t count;
	uint32_t start;
};


double terrs[NUM_THREADS];

typedef hash_multimap<int, int> multimapII;
hash_map<int, int> genreMap;
hash_multimap<int, int> genreTrackMap;
unsigned int umaplen = 0,imaplen = 0;
hash_map<int, int> imap;
hash_map<int, int> umap;
double *bu, *bi, *p, *x, *y, *q, *bg;
struct rating_s *ratings;
struct item_s *items;
struct user_s *users;
struct rating_s *validations;
struct rating_s *tests;
double mu;

pthread_barrier_t barrier;

inline double collect_errors(double err, int rank) {
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

void *open_rw(const char *file, ssize_t size)
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
	madvise(data, size, MADV_SEQUENTIAL);
	return data;
}


void load_model()
{
	bg = (double*) open_rw("tmp/bg.mmap", sizeof(double)*nGenres);
	bi = (double*) open_rw("tmp/bi.mmap", sizeof(double)*nItems);
	bu = (double*)open_rw("tmp/bu.mmap", sizeof(double)*nUsers);
	ratings = (struct rating_s*)open_rw("tmp/ratings.mmap", sizeof(rating_s)*nRatings);
	validations= (struct rating_s*)open_rw("tmp/validations.mmap", sizeof(rating_s)*nValidations);
	tests = (struct rating_s*)open_rw("tmp/tests.mmap", sizeof(rating_s)*nTests);
	items = (struct item_s*)open_rw("tmp/items.mmap", sizeof(item_s)*nItems);
	users = (struct user_s*)open_rw("tmp/users.mmap", sizeof(user_s)*nUsers);

	p = (double *)open_rw("tmp/p.mmap", sizeof(double)*nFeatures*nUsers);
	q = (double*)open_rw("tmp/q.mmap", sizeof(double)*nFeatures*nItems);
	x = (double*)open_rw("tmp/x.mmap", sizeof(double)*nFeatures*nItems);
	y = (double*)open_rw("tmp/y.mmap", sizeof(double)*nFeatures*nItems);
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
	double sq, err=1e6, pred, lasterr = 1e6+1, faults = 0;
	int epoch = 0;
	while (epoch < maxepochsbias && faults < maxfaults) {
		double start = get_time();
		lasterr = err;
		sq = 0;
		for(int u = 0; u < nUsers; u++) {
			for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
				struct rating_s rating = ratings[r];
				double tmpbi = bi[rating.item];
				pred = mu + bu[u] + tmpbi;
				pair<multimapII::const_iterator, multimapII::const_iterator> p =
					genreTrackMap.equal_range(rating.item);
				pred = mu + bu[u] + bi[rating.item];
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					pred += bg[(*i).second];
				}
				if (pred < 0) pred = 0;
				if (pred > 100./SCORENORM) pred = 100./SCORENORM;
				err = rating.rating - pred;
				sq += err*err*SCORENORM*SCORENORM;
				bu[u] += userStep*(err - userReg*bu[u]);
				bi[rating.item] += itemStep*(err - itemReg*bi[rating.item]);
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					int gid = (*i).second;
					bg[gid] += genreStep*(err - genreReg*bg[gid]);
				}

				pthread_mutex_lock(&mutexB);
				bi[rating.item] += GAMMA*(err - LAMBDA*tmpbi);
				pthread_mutex_unlock(&mutexB);
				//printf("%g %g %g\n", err, rating.rating, pred);
				//printf("%g\n", bi[ratings[r].item]);

			}
		}
		collect_errors(sq, rank);
		sq = sqrt(sq / (double)nRatings);
		double vsq = 0;
		for(int u = rank; u < nUsers; u+=NUM_THREADS) {
			for(int r = u*4; r < (u+1)*4; r++) {
				struct rating_s rating = validations[r];
				pair<multimapII::const_iterator, multimapII::const_iterator> p =
					genreTrackMap.equal_range(rating.item);

				pred = mu + bu[u] + bi[rating.item];
				//printf("%g %g %g %d ", mu, bu[u], bi[rating.item],rating.item);
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					int gid = (*i).second;
					pred += bg[gid];
					//printf("%g ", bg[gid]);
				}
				//printf("\n");
				if (pred < 0) pred = 0;
				if (pred > 100./SCORENORM) pred = 100./SCORENORM;
				err = rating.rating - pred;
				vsq += err*err*SCORENORM*SCORENORM;
			}
			//printf("%g %d %g\n", err, validations[i].rating, pred);
		}

		vsq = collect_errors(vsq, rank);
		vsq = sqrt(vsq / (double)nValidations);
		err = vsq;

		if (rank == 0 ) {
			printf("BIAS epoch=%d RMSE=%g VRMSE=%g time=%g\n", epoch, sq, vsq, get_time()-start, sq);
		}
		if (err > lasterr) {
			faults++;
		} else {
			genreStep *= decay;
			itemStep *= decay;
			userStep *= decay;
			faults = 0;
		}
		epoch++;
	}

	for(int i = rank; i < nUsers*nFeatures;i += NUM_THREADS) {
		p[i] =  0; //randF(-.003, .001);
		//p[i] = 1/sqrt(nFeatures) * (randF(-.5,.5)); //randF(-.003,-.003); //.003, .001);
	}
	for(int i = rank; i < nItems*nFeatures;i+=NUM_THREADS) {
		const double total = .01,  min=-.5, max=.5;
		q[i] = 0;// total/sqrt(nFeatures) * (randF(min, max));
		x[i] = 0; //total/sqrt(nFeatures) * (randF(min, max));
		y[i] = 0;// total/sqrt(nFeatures) * (randF(min, max));
	}

	pthread_barrier_wait(&barrier);
	double sum = 0;
	for(int u = rank; u < nUsers; u += NUM_THREADS) {
		for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
			ratings[r].extra = ratings[r].rating - mu -bu[u] - bi[ratings[r].item];
			pair<multimapII::const_iterator, multimapII::const_iterator> p = genreTrackMap.equal_range(ratings[r].item);
			for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
				int gid = (*i).second;
				ratings[r].extra -= bg[gid];
			}
			sum += ratings[r].extra;
		}
	}
	sum = collect_errors(sum, rank);
	if (rank == 0 ) {
		printf("Average deviation: %g\n", sum/nRatings);
	}
	return 0;
}

double predict(int uid, int iid)
{
	double sum = mu;
	sum += bu[uid];
	sum += bi[iid];
	for(int f = 0; f < nFeatures; f++) {
	//	sum += q[iid*nFeatures+f] * p[uid*nFeatures+f];
	}
	pair<multimapII::const_iterator, multimapII::const_iterator> gp =
		genreTrackMap.equal_range(iid);
	for (multimapII::const_iterator i = gp.first; i != gp.second; ++i) {
		int gid = (*i).second;
		sum += bg[gid];
	}
	if (sum > 100./SCORENORM) sum = 100. /SCORENORM ;
	if (sum < 0) sum = 0;
	return sum; // * 100;
}

double validate(bool print = false)
{
	double sq = 0;
	for(int u = 0; u < nUsers; u++) {
		for(int r = u*4; r < (u+1)*4; r++) {
			double pred = predict(u, validations[r].item);
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
			double pred = predict(u,tests[r].item);
			fprintf(fp, "%lg\n", pred*SCORENORM);
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
			score = (double) stmp / SCORENORM;
			if( imap.count(iid) == 0) {
				iidx = imaplen;
				imaplen ++;
				imap[iid] = iidx;

				items[iidx].id = iid;
				items[iidx].count = 0;
			} else {
				iidx = imap[iid];
			}
			tests[ridx].item = iidx;
			tests[ridx].rating = stmp;
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
			} else {
				iidx = imap[iid];
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
	double lasterr = 1e6+1, sq = 0, err = 1e6;
	int epoch = 0, faults = 0;
	printf("maxf=%d\n", maxfaults);
	while (faults <  maxfaults && epoch < maxepochs) {
	//while (epoch < maxepochs) { // && (epochs < 10 || (lasterr - err) > 1e-6))
		lasterr = err;
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
				struct rating_s rating = ratings[r];
				double tmpbi = bi[rating.item];
				double pred = mu + tmpbi + bu[u];
				pair<multimapII::const_iterator, multimapII::const_iterator> gp = genreTrackMap.equal_range(rating.item);
				for (multimapII::const_iterator i = gp.first; i != gp.second; ++i) {
					int gid = (*i).second;
					pred += bg[gid];
				}
				for(int f1 = 0; f1 < nFeatures; f1++) {
					pred += p[uf + f1]*q[rating.item*nFeatures+f1];
				}
				for(int f = 0; f < nFeatures; f++) {
					//for(int f1 = 0; f1 < nFeatures; f1++) {
					pred += p[uf + f]*q[rating.item*nFeatures+f];
					//}
					err = rating.rating - pred;
					if(f == nFeatures - 1)
						sq += err*err*SCORENORM*SCORENORM;

					double tmpq = q[rating.item*nFeatures+f];
					double tmpp = p[uf + f];
					sum[f] += err*tmpq;
					q[rating.item*nFeatures+f] += GAMMA*(err*tmpp - LAMBDA*tmpq);
					p[uf+f] += GAMMA*(err*tmpq - LAMBDA*tmpp);
				}
				bu[u] += GAMMA*(err - LAMBDA*bu[u]);
				pthread_mutex_lock(&mutexB);
				bi[rating.item] += GAMMA*(err - LAMBDA*tmpbi);
				pthread_mutex_unlock(&mutexB);
			}


			for(int f = 0; f < nFeatures; f++) {
				pthread_mutex_lock(&mutexFeature[f]);
				for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
					struct rating_s rating = ratings[r] ;
					float rb = ratings[r].extra;
					int i = rating.item*nFeatures + f;
					x[i] += GAMMA2*(invsqru*rb*sum[f] - LAMBDA2*x[i]);
					y[i] += GAMMA2*(invsqnu*sum[f] - LAMBDA2*y[i]);
				}
				for(int r = 4*u; r < 4*(u+1); r++) {
					struct rating_s rating = validations[r];
					int i = rating.item*nFeatures + f;
					y[i] += GAMMA2*(invsqnu*sum[f] - LAMBDA2*y[i]);
				}
				for(int r = 6*u; r < 6*(u+1); r++) {
					struct rating_s rating = tests[r];
					int i = rating.item*nFeatures + f;
					y[i] += GAMMA2*(invsqnu*sum[f] - LAMBDA2*y[i]);
				}
				pthread_mutex_unlock(&mutexFeature[f]);
			}
		}
		sq = collect_errors(sq, rank);
		err = sqrt(sq / nRatings);
		double verr = validate();
		if (rank == 0) {
			printf("epoch=%d RMSE=%g VMRSE=%g" , epoch, err, verr);
			printf(" time=%g rank=%d\n", get_time()-start, rank);

		}
		epoch++;
		if (verr > lasterr) {
			faults++;
		} else {
			GAMMA *= decay;
			GAMMA2 *= decay;
			faults = 0;
		}
		err = verr;
	}
	return 0;
}

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
			//users[uidx].sum = 0;
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
			} else {
				iidx = imap[iid];
			}
			items[iidx].count += 1;
			ratings[ridx].item = iidx;
			ratings[ridx].rating = score;
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
		} else {
			fprintf(stderr, "ERROR unknown value in stats1.txt\n");
			exit(1);
		}
		fprintf(stdout, "%s=%lu\n", name, d);
	}

	fclose(fp);
}

void read_trackData()
{
	FILE *fp = fopen("trackData1.txt","r");
	int lines = 0, value;
	char *cur;
	char buf[200];
	int trackid, albumid, artistid;
	int genLen = 0;
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
			albumid = -1;
		} else {
			albumid = atoi(cur);
		}
		cur = strtok(NULL, "|");
		if (cur[0] == 'N') {
			artistid = -1;
		} else {
			artistid = atoi(cur);
		}
		while( (cur = strtok(NULL, "|")) != NULL) {
			int value = atoi(cur);
			int gid;
			if ( genreMap.count(value) == 0) {
				genreMap[value] = genLen++;
			}
			gid = genreMap[value];
			genreTrackMap.insert(hash_multimap<int,int>::value_type(iid, gid));
		}
		lines++;
	}
	//printf("'%s' %d\n", buf, genLen);
	//assert(genLen == nGenres);
}

void read_genres()
{
	FILE *fp = fopen("genreData1.txt","r");
	int lines = 0, value;
	while(! feof(fp) ) {
		fscanf(fp, "%d\n", &value);
		if(feof(fp)) break;
		if(imap.count(value) == 0)
			continue;
		int iid = imap[value];
		items[iid].type = GENRE;
		lines++;
	}
}

void print_model_stats()
{
	printf("--------------\n");
	printf("Stats\n");
	printf("--------------\n");
	for(int i = 0; i < nFeatures; i++) {
		printf("%g %g %g %g\n", p[i], q[i], x[i], y[i]);
	}
	printf("MEAN(global) = %g\n", mu);
	double a = 0;
	for(int i = 0; i < nUsers; i++) {
		a += bu[i];
	}
	printf("MEAN(bu) = %g\n", a/(double)nUsers);
	double b = 0;
	for(int i = 0; i < nItems; i++) {
		b += bi[i];
	}
	printf("MEAN(bi) = %g\n", b/(double)nItems);
	printf("--------------\n");
}

void make_tmp_dir()
{
	struct stat st;
	if(stat("tmp",&st) == 0)
		return;
	else
		mkdir("tmp", 0777);
}

int main (int argc, char **argv)
{
	srandom(time(NULL));
	int c;
	bool isInit = false;
	bool isTrain = false;
	bool isVerify = false;
	bool isBuildModel = false;
	bool isStats = false;
	bool isPredict = false;

	while ( (c = getopt(argc, argv, "itvme:spf:bE:")) != -1) {
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
		printf("VRMSE = %g\n", validate(true));
	}

	if(isPredict) {
		make_predictions();
	}
	pthread_barrier_destroy(&barrier);
	pthread_exit(NULL);

	exit (0);
}

