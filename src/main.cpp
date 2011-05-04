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

using namespace std;
using namespace __gnu_cxx;

double itemStep		= 0.005;
double itemReg		= 1;
double userStep		= 1.5;
double userReg		= 1;

double GAMMA = .005; //.07;
double LAMBDA = 1; //0.05;
double GAMMA2 = .005 ; // / 10000;//.0001;
double LAMBDA2 =  .4;//0005; //100; //GAMMA2*40;


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
#define VFILE  "validationIdx1.txt"
#define TFILE  "trainIdx1.txt"
#define TESTFILE  "testIdx1.txt"


const unsigned int nFeatures = 100;

struct rating_s {
	//unsigned int user : 20;
	unsigned int item : 20;
	unsigned int rating : 8;
	unsigned int type : 3;
};

struct item_s {
	uint32_t id;
	uint32_t count;
};

struct user_s {
	uint32_t id;
	uint32_t count;
	uint8_t nu;
	uint8_t qu;
};


double terrs[NUM_THREADS];

unsigned int umaplen = 0,imaplen = 0;
hash_map<int, int> imap;
hash_map<int, int> umap;
double *bu, *bi, *p, *x, *y, *q;
struct rating_s *ratings;
struct item_s *items;
struct user_s *users;
struct rating_s *validations;
struct rating_s *tests;
double mu;

pthread_barrier_t barrier;

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
	double sq, err=1e6, pred, lasterr = 1e6+1, faults = 0;
	int epoch=0;
	while (epoch < maxepochsbias && faults < maxfaults) {
		double start = get_time();
		lasterr = err;
		sq = 0;
		int ridx = 0;
		for(int u = 0; u < nUsers; u++) {
			for(int r = 0; r < users[u].count; r++,ridx++) {
				struct rating_s rating = ratings[ridx];
				pred = mu + bu[u] + bi[rating.item];
				if (pred < 0) pred = 0;
				if (pred > 100./SCORENORM) pred = 100./SCORENORM;
				err = (double)rating.rating/SCORENORM - pred;
				sq += err*err*SCORENORM*SCORENORM;
				bu[u] += userStep*(err - userReg*bu[u]);
				bi[rating.item] += itemStep*(err - itemReg*bi[rating.item]);
				//printf("%g\n", bi[ratings[r].item]);

			}
		}
		sq = sqrt(sq / (double)nRatings);
		double vsq = 0;
		ridx = 0;
		for(int u = 0; u < nUsers; u++) {
			for(int i = 0; i < users[u].nu; i++, ridx++) {
				struct rating_s rating=validations[ridx];
				pred = mu + bu[u] + bi[rating.item];
				if (pred < 0) pred = 0;
				if (pred > 100./SCORENORM) pred = 100./SCORENORM;
				err = (double)rating.rating/SCORENORM - pred;
				vsq += err*err*SCORENORM*SCORENORM;
			}
			//printf("%g %d %g\n", err, validations[i].rating, pred);
		}
		vsq = sqrt(vsq / (double)nValidations);
		if (rank == 0 ) {
			printf("BIAS epoch=%d RMSE=%g VRMSE=%g time=%g\n", epoch, sq, vsq, get_time()-start, sq);
		}
		err = vsq;
		if (err > lasterr) {
			faults++;
		} else {
			itemStep *= .7;
			userStep *= .7;
			faults = 0;
		}
		epoch++;
	}
	for(int i = rank; i < nUsers*nFeatures;i+=NUM_THREADS) {
		p[i] =  0; //randF(-.003, .001);
		p[i] = .01/sqrt(nFeatures) * (randF(-.5,.5)); //randF(-.003,-.003); //.003, .001);
	}
	for(int i = rank; i < nItems*nFeatures;i+=NUM_THREADS) {
		const double total = .025, min=-2., max=1.;
		q[i] = total/sqrt(nFeatures) * (randF(min, max));
		x[i] = total/sqrt(nFeatures) * (randF(min, max));
		y[i] = total/sqrt(nFeatures) * (randF(min, max));
	}
	return 0;
}

double predict(int uid, int iid)
{
	double sum = mu;
	sum += bu[uid];
	sum += bi[iid];
	for(int f = 0; f < nFeatures; f++) {
		sum += q[iid*nFeatures+f] * p[uid*nFeatures+f];
	}
	if (sum > 100./SCORENORM) sum = 100. /SCORENORM ;
	if (sum < 0) sum = 0;
	//printf("* %g %g \n", mu + bu[uid] + bi[iid], sum);

	return sum; // * 100;
}

double validate(bool print = false)
{
	double sq = 0;
	int ridx= 0;
	for(int u = 0; u < nUsers; u++) {
		for(int i = 0; i < users[u].nu; i++,ridx++){
			double pred = predict(u, validations[ridx].item);
			double err = (double)validations[ridx].rating/SCORENORM - pred;
			sq += err*err*SCORENORM*SCORENORM;
			if (print) {
				printf("%d %d %d %g %g\n", u, validations[ridx].item, validations[ridx].rating, pred*SCORENORM, err*SCORENORM);
			}
		}
	}
	return sqrt(sq/nValidations);

}

void make_predictions()
{
	FILE *fp = fopen("predictions.txt", "w");
	int ridx=0;
	for(int u = 0; u < nUsers; u++) {
		for(int r = 0; r < users[u].qu; r++) {
			double pred = predict(u,tests[ridx].item);
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
	char *data = (char *)open_read(TESTFILE, &size);
	int sum = 0;
	while(1) {
		if ( parseInt(data, &didx, &uid, size) ) {
			break;
		}
		uidx = umap[uid];
		parseInt(data, &didx, &n, size);
		if (n != 6) printf("Error size not 6!=%d\n", n);
		users[uidx].qu = n;
		for(int i =0; i < n; i++) {
			parseInt(data, &didx, &iid, size);
			parseInt(data, &didx, &day, size);
			parseInt(data, &didx, &hr, size);
			parseInt(data, &didx, &min, size);
			parseInt(data, &didx, &sec, size);
			score = (double) stmp;
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
	char *data = (char *)open_read(VFILE, &size);
	int sum = 0;
	while(1) {
		if ( parseInt(data, &didx, &uid, size) ) {
			break;
		}
		uidx = umap[uid];
		parseInt(data, &didx, &n, size);
		users[uidx].nu = n;
		for(int i =0; i < n; i++) {
			parseInt(data, &didx, &iid, size);
			parseInt(data, &didx, &stmp, size);
			parseInt(data, &didx, &day, size);
			parseInt(data, &didx, &hr, size);
			parseInt(data, &didx, &min, size);
			parseInt(data, &didx, &sec, size);
			score = (double) stmp;
			if( imap.count(iid) == 0) {
				iidx = imaplen;
				imaplen ++;
				imap[iid] = iidx;

				items[iidx].id = iid;
				items[iidx].count = 0;
				//items[iidx].sum = 0;
				//iidx = -1;
			} else {
				iidx = imap[iid];
			}
			validations[ridx].item = iidx;
			validations[ridx].rating = stmp; //score;
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

		int ridx = 0;
		for(int uidx = 0; uidx < nUsers; uidx++) {
			struct user_s user = users[uidx];
			double invsqru = 0;
			double invsqnu = 0;
			if (user.count > 0) {
				invsqru = sqrt( 1. / (double)user.count);
			}
			invsqnu = sqrt( 1. / (double)(user.nu +user.count+user.qu));
			int uf =  user.id*nFeatures;
			for(int f = 0; f < nFeatures; f++) {
				p[uf + f] = 0;

				for(int r = 0; r < user.count; r++) {
					float rb = (double)ratings[ridx+r].rating / SCORENORM;
					rb -= mu;
					rb -= bu[uidx];
					rb -= bi[ratings[ridx+r].item];
					int j = (ratings[ridx+r].item*nFeatures);
					p[uf+f] += rb * x[j+f] * invsqru;
					p[uf+f] += y[j+f] * invsqnu;
				}
				for(int r = uidx*4; r < (uidx+1)*4; r++) {
					int j = (validations[r].item*nFeatures);
					p[uf+f] += y[j+f] * invsqnu;
				}
				for(int r = uidx*6; r < (uidx+1)*6; r++) {
					int j = (tests[r].item*nFeatures);
					p[uf+f] += y[j+f] * invsqnu;
				}
				sum[f] = 0.;
			}
			//pthread_barrier_wait(&barrier);
			for(int r = 0; r < user.count; r++ ) {
				struct rating_s rating = ratings[ridx+r];
				double pred = mu + bi[rating.item] + bu[uidx];
				for(int f = 0; f < nFeatures; f++) {
				//for(int f1 = 0; f1 < nFeatures; f1++) {
					pred += p[uf + f]*q[rating.item*nFeatures+f];
				//}
					err = rating.rating / SCORENORM - pred;
					if(f == nFeatures - 1)
						sq += err*err*SCORENORM*SCORENORM;


					double tmpq = q[rating.item*nFeatures+f];
					double tmpp = p[uf + f];

					sum[f] += err*tmpq;

					q[rating.item*nFeatures+f] += GAMMA*(err*tmpp - LAMBDA*tmpq);
					//p[uidx*nFeatures+f] += GAMMA*(err*tmpq - LAMBDA*tmpp);
				}
				bu[uidx] += GAMMA*(err - LAMBDA*bu[uidx]);
				bi[rating.item] += GAMMA*(err - LAMBDA*bi[rating.item]);
			}


			for(int r = 0; r < user.count; r++) {
				struct rating_s rating = ratings[ridx + r] ;
				float rb = (double)rating.rating/SCORENORM - mu -bu[uidx] -bi[rating.item];
				for(int f = 0; f < nFeatures; f++) {
					int i = rating.item*nFeatures + f;
					x[i] += GAMMA2*(invsqru*rb*sum[f] - LAMBDA2*x[i]);
					y[i] += GAMMA2*(invsqnu*sum[f] - LAMBDA2*y[i]);
				}
			}
			for(int r = 4*uidx; r < 4*(uidx+1); r++) {
				struct rating_s rating = validations[r];
				for(int f = 0; f < nFeatures; f++) {
					int i = rating.item*nFeatures + f;
					y[i] += GAMMA2*(invsqnu*sum[f] - LAMBDA2*y[i]);
				}
			}
			for(int r = 6*uidx; r < 6*(uidx+1); r++) {
				struct rating_s rating = tests[r];
				for(int f = 0; f < nFeatures; f++) {
					int i = rating.item*nFeatures + f;
					y[i] += GAMMA2*(invsqnu*sum[f] - LAMBDA2*y[i]);
				}
			}
			ridx += user.count;
		}
		//pthread_barrier_wait(&barrier);
		double verr = validate();
		err = sqrt(sq / nRatings);
		printf("epoch=%d RMSE=%g VMRSE=%g" , epoch++, err, verr);
		printf(" time=%g rank=%d\n", get_time()-start, rank);
		if (verr > lasterr) {
			faults++;
		} else {
			GAMMA *= .7;
			//GAMMA2 *= .7;
			faults = 0;
		}
		err= verr;
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
	char *data = (char *)open_read(TFILE, &size);
	int sum = 0;
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
		for(int i =0; i < n; i++) {
			parseInt(data, &didx, &iid, size);
			parseInt(data, &didx, &stmp, size);
			parseInt(data, &didx, &day, size);
			parseInt(data, &didx, &hr, size);
			parseInt(data, &didx, &min, size);
			parseInt(data, &didx, &sec, size);
			score = (double) stmp;
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
	mu /= (double)ridx * SCORENORM;
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
	hash_multimap<int, int> genreMap;
	FILE *fp = fopen("trackData1.txt","r");
	int lines = 0, value;
	char *cur;
	char buf[200];
	int trackid, albumid, artistid;
	while(! feof(fp) ) {
		fgets(buf, 200, fp);
		trackid = atoi(strtok(buf, "|"));
		cur = strtok(NULL, "|");
		if (cur[0] == 'N') {
			albumid = -1;
		} else {
			albumid = atoi(cur);
		}
		if (cur[0] == 'N') {
			artistid = -1;
		} else {
			artistid = atoi(cur);
		}
		while( (cur = strtok(NULL, "|")) != NULL) {
			genreMap
		}
		printf("%s\n", buf);
		lines++;
	}
	printf("%d\n", lines);
}

void read_genres()
{
	FILE *fp = fopen("genreData1.txt","r");
	int lines = 0, value;
	while(! feof(fp) ) {
		fscanf(fp, "%d\n", &value);
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

	//read_genres();
	read_trackData();
	return 0;

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

	read_stats();

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

