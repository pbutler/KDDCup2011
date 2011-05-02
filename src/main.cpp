#include <stdint.h>
#include <cmath>
#include<stdio.h>
#include<stdlib.h>
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


double itemStep				= 0.005;
double itemReg					= 1;
double userStep				= 1.5;
double userReg					= 1;

double GAMMA = .005; //.07;
double GAMMA2 = .001; // / 10000;//.0001;
double LAMBDA = 0.05;
double LAMBDA2 =  GAMMA2*40;
#define NUM_THREADS 1
using namespace __gnu_cxx;
using namespace std;

#define SCORENORM  100

int maxepochs = 10;
#define TEST 1
#undef TEST
#ifndef TEST
unsigned int nUsers = 1000990;
unsigned int nItems = 624961;
unsigned int nRatings = 252800275;
unsigned int nValidations = nUsers*4;
#define VFILE  "../track1/validationIdx1.txt"
#define TFILE  "../track1/trainIdx1.txt"
#else
unsigned int nUsers = 44;
unsigned int nItems = 624961;
unsigned int nRatings = 11696;
unsigned int nValidations = nUsers*4;
#define VFILE  "../testdata/validationIdx1.txt"
#define TFILE  "../testdata/trainIdx1.txt"
#endif

const unsigned int nFeatures = 50;

struct rating_s {
	unsigned int user : 20;
	unsigned int item : 20;
	float rating;
	//unsigned int rating : 8;
};

struct item_s {
	uint32_t id;
	uint32_t count;
};

struct user_s {
	uint32_t id;
	uint32_t count;
	uint32_t nu;
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

void *open_read(char *file, ssize_t *size)
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

void *open_rw(char *file, ssize_t size)
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
	while (faults <= 2) {
		double start = get_time();
		lasterr = err;
		sq = 0;
		for(int r = rank; r < nRatings; r+=NUM_THREADS ) {
			pred = mu + bu[ratings[r].user] + bi[ratings[r].item];
			if (pred < 0) pred = 0;
			if (pred > 100/SCORENORM) pred = 100/SCORENORM;
			err = ratings[r].rating - pred;
			sq += err*err*SCORENORM*SCORENORM;
			bu[ratings[r].user] += userStep*(err - userReg*bu[ratings[r].user]);
			bi[ratings[r].item] += itemStep*(err - itemReg*bi[ratings[r].item]);
			//printf("%g\n", bi[ratings[r].item]);
		}
		sq = sqrt(sq / (double)nRatings);
		double vsq = 0;
		for(int i = 0; i < nValidations; i++) {
			pred = mu + bu[validations[i].user];
			if (items[validations[i].item].count > 0) {
				pred += bi[validations[i].item];
			}
			if (pred < 0) pred = 0;
			if (pred > 100/SCORENORM) pred = 100/SCORENORM;
			err = validations[i].rating - pred;
			vsq += err*err*SCORENORM*SCORENORM;
			//printf("%g %g %g\n", err, validations[i].rating, pred, bu[ratings[i].user]);
		}
		vsq = sqrt(vsq / (double)nValidations);
		if (rank == 0 ) {
			printf("BIAS RMSE=%g VRMSE=%g time=%g %g\n", sq, vsq, get_time()-start, sq);
		}
		err = vsq;
		if (err > lasterr) {
			faults++;
		} else {
			itemStep *= .7;
			userStep *= .7;
			faults = 0;
		}
	}
	for(int i = rank; i < nUsers*nFeatures;i+=NUM_THREADS) {
		p[i] =  0; //randF(-.003, .001);
		p[i] = .01/sqrt(nFeatures) * (randF(-.5,.5)); //randF(-.003,-.003); //.003, .001);
	}
	for(int i = rank; i < nItems*nFeatures;i+=NUM_THREADS) {
		q[i] = .01/sqrt(nFeatures) * (randF(-.5,.5)); //randF(-.003,-.003); //.003, .001);
		x[i] = .01/sqrt(nFeatures) * (randF(-.5,.5)); //randF(-.003,-.003); //.003, .001);
		y[i] = .01/sqrt(nFeatures) * (randF(-.5,.5)); //randF(-.003,-.003); //.003, .001);
	}
	return 0;
}

double predict(int uid, int iid)
{
	double sum = mu;
	sum += bu[uid];
	if ( items[iid].count > 0) {
		sum += bi[iid];
		for(int f = 0; f < nFeatures; f++) {
			sum += q[iid*nFeatures+f] * p[uid*nFeatures+f];
		}
	}
	if (sum > 100/SCORENORM) sum = 100 /SCORENORM ;
	if (sum < 0) sum = 0;
	//printf("* %g %g \n", mu + bu[uid] + bi[iid], sum);

	return sum; // * 100;
}

double validate(bool print = false)
{
	double sq = 0;
	for(int i = 0; i < nValidations; i++) {
		double pred = predict(validations[i].user, validations[i].item);
		double err = validations[i].rating - pred;
		sq += err*err*SCORENORM*SCORENORM;
		if (print) {
			printf("%d %d %g %g %g\n", validations[i].user, validations[i].item, validations[i].rating, pred, err);
		}
	}
	return sqrt(sq/nValidations);

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
			score /= SCORENORM;
			//printf("%f\n", score);
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
			//users[uidx].count += 1;
			//users[uidx].sum += score;
			//items[iidx].count += 1;
			//items[iidx].sum += score;
			validations[ridx].item = iidx;
			validations[ridx].user = uidx;
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
	while (faults <= 2 && epoch < maxepochs) {
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
			if(user.nu > 0) {
				invsqnu = sqrt( 1. / (double)user.nu);
			}
			int uf =  user.id*nFeatures;
			for(int f = 0; f < nFeatures; f++) {
				p[uf + f] = 0;

				for(int r = 0; r < user.count; r++) {
					float rb = ratings[ridx+r].rating;
					rb -= mu;
					rb -= bu[ratings[ridx+r].user];
					rb -= bi[ratings[ridx+r].item];
					int j = (ratings[ridx+r].item*nFeatures);
					p[uf+f] += rb * x[j+f] * invsqru;
					p[uf+f] += y[j+f] * invsqnu;
				}
				sum[f] = 0.;
			}
			//pthread_barrier_wait(&barrier);
			for(int r = 0; r < user.count; r++ ) {
				struct rating_s rating = ratings[ridx+r];
				double pred = mu + bi[rating.item] + bu[rating.user];
				for(int f1 = 0; f1 < nFeatures; f1++) {
					pred += p[rating.user*nFeatures + f1]*q[rating.item*nFeatures+f1];
				}
				err = rating.rating - pred;
				sq += err*err*SCORENORM*SCORENORM;

				for(int f = 0; f < nFeatures; f++) {

					double tmpq = q[rating.item*nFeatures+f];
					double tmpp = p[rating.user*nFeatures+f];

					sum[f] += err*tmpq;

					q[rating.item*nFeatures+f] += GAMMA*(err*tmpp - LAMBDA*tmpq);
					//p[rating.user*nFeatures+f] += GAMMA*(err*tmpq - LAMBDA*tmpp);
				}
				bu[rating.user] += GAMMA*(err - LAMBDA*bu[rating.user]);
				bi[rating.item] += GAMMA*(err - LAMBDA*bi[rating.item]);
			}


			for(int r = 0; r < user.count; r++) {
				struct rating_s rating = ratings[ridx + r];
				float rb = rating.rating - mu -bu[rating.user] -bi[rating.item];
				for(int f = 0; f < nFeatures; f++) {
					int i = rating.item*nFeatures + f;
					x[i] += GAMMA2*(invsqru*rb*sum[f] - LAMBDA2*x[i]);
					y[i] += GAMMA2*(invsqnu*sum[f] - LAMBDA2*y[i]);
				}
			}
			ridx += user.count;
		}
		//pthread_barrier_wait(&barrier);
		double verr = validate();
		err = sqrt(sq / nRatings);
		printf("epoch=%d RMSE=%g VMRSE=%g" , epoch++, err, verr);
		printf(" time=%g rank=%d\n", start-get_time(), rank);
		if (verr > lasterr) {
			faults++;
		} else {
			GAMMA *= .7;
			GAMMA2 *= .7;
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
			users[uidx].count = 0;
			//users[uidx].sum = 0;
		} else {
			uidx = imap[uid];
		}
		parseInt(data, &didx, &n, size);
		for(int i =0; i < n; i++) {
			parseInt(data, &didx, &iid, size);
			parseInt(data, &didx, &stmp, size);
			parseInt(data, &didx, &day, size);
			parseInt(data, &didx, &hr, size);
			parseInt(data, &didx, &min, size);
			parseInt(data, &didx, &sec, size);
			score = (double) stmp;
			mu += score;
			score /= SCORENORM;
			if( imap.count(iid) == 0) {
				iidx = imaplen;
				imaplen ++;
				imap[iid] = iidx;

				items[iidx].id = iid;
				items[iidx].count = 0;
				//items[iidx].sum = 0;
			} else {
				iidx = imap[iid];
			}
			users[uidx].count += 1;
			//users[uidx].sum += score;
			items[iidx].count += 1;
			//items[iidx].sum += score;
			ratings[ridx].item = iidx;
			ratings[ridx].user = uidx;
			ratings[ridx].rating = score;
			ridx++;
		}
	}
	munmap(data, size);
	read_validate();
	nItems = imaplen;
	nUsers = umaplen;
	nRatings = ridx;
	mu /= (double)ridx; //  * SCORENORM;
	printf("nRatings = %d\n", nRatings);
	printf("mu = %lg\n", mu);

	FILE *tmp = fopen("tmp/values", "w");
	fprintf(tmp, "%u %u %u %25.18g\n", nRatings, nUsers, nItems, mu);
	for(int i = 0 ; i < nUsers; i++ ) {
		fprintf(tmp, "%d\n", users[i].id);
	}
	for(int i = 0 ; i < nItems; i++ ) {
		fprintf(tmp, "%d\n", items[i].id);
	}
	fclose(tmp);
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
	while ( (c = getopt(argc, argv, "itvme:s")) != -1) {
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

	if(!isInit) {
		FILE *tmp = fopen("tmp/values", "r");
		fscanf(tmp, "%u %u %u %lg\n", &nRatings, &nUsers, &nItems, &mu);
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

	printf("<");
	load_model();
	printf(">\n");

	printf("<"); fflush(stdout);
	if(isInit) {
		read_data();
	}
	printf(">\n");

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
	pthread_barrier_destroy(&barrier);
	pthread_exit(NULL);

	exit (0);
}
