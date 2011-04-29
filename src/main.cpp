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

const float GAMMA = .02;
const float LAMBDA = .04;
#define NUM_THREADS 1
using namespace __gnu_cxx;
using namespace std;

#define SCORENORM  100

int maxepochs = 10;
#ifdef TEST
unsigned int nUsers = 1000990;
unsigned int nItems = 624691;
unsigned int nRatings = 252800275;
#define VFILE  "../track1/validationIdx1.txt"
#define TFILE  "../track1/trainIdx1.txt"
#else
unsigned int nUsers = 44;
unsigned int nItems = 624961;
unsigned int nRatings = 11696;
#define VFILE  "../testdata/validationIdx1.txt"
#define TFILE  "../testdata/trainIdx1.txt"
#endif

const unsigned int nFeatures = 1;

struct rating_s {
	uint32_t user;
	uint32_t item;
	float rating;
	float cache;

};

struct item_s {
	uint32_t id;
	uint32_t count;
	float sum;
};

struct user_s {
	uint32_t id;
	uint32_t count;
	float sum;
};


float terrs[NUM_THREADS];

unsigned int umaplen = 0,imaplen = 0;
hash_map<int, int> imap;
hash_map<int, int> umap;
float *bu, *bi, mu, *p, *x, *y, *q;
struct rating_s *ratings;
struct item_s *items;
struct user_s *users;

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

inline int parseInt(char *s, int *i, int *a, int size) {
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
		printf("%d\n", errno);
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
		printf("%d\n", errno);
		if ( errno == EACCES) printf("ACCESS\n");
		if ( errno == EBADF) printf("BADF\n");
		if ( errno == EINVAL) printf("INVAL\n");
		return NULL;
	}
	return data;
}


void load_model()
{
	bi = (float*) open_rw("tmp/bi.mmap", sizeof(float)*nItems);
	bu = (float*)open_rw("tmp/bu.mmap", sizeof(float)*nUsers);
	ratings = (struct rating_s*)open_rw("tmp/ratings.mmap", sizeof(rating_s)*nRatings);
	items = (struct item_s*)open_rw("tmp/items.mmap", sizeof(item_s)*nItems);
	users = (struct user_s*)open_rw("tmp/users.mmap", sizeof(user_s)*nUsers);

	p = (float *)open_rw("tmp/p.mmap", sizeof(user_s)*nFeatures*nUsers);
	q = (float*)open_rw("tmp/q.mmap", sizeof(user_s)*nFeatures*nItems);
	x = (float*)open_rw("tmp/x.mmap", sizeof(user_s)*nFeatures*nItems);
	y = (float*)open_rw("tmp/y.mmap", sizeof(user_s)*nFeatures*nItems);
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
	float sq, err=1e6, pred, lasterr = 1e6+1;
	while (lasterr - err > 1e-6) {
		double start = get_time();
		lasterr = err;
		sq = 0;
		for(int r = rank; r < nRatings; r+= NUM_THREADS) {
			pred = mu + bu[ratings[r].user] + bi[ratings[r].item];
			err = ratings[r].rating - pred;
			sq += err*err*SCORENORM*SCORENORM;

			bu[ratings[r].user] += GAMMA*(err - LAMBDA*bu[ratings[r].user]);
			bi[ratings[r].item] += GAMMA*(err - LAMBDA*bi[ratings[r].item]);
		}
		terrs[rank] = sq;
		pthread_barrier_wait(&barrier);
		sq = 0;
		for(int i = 0; i<NUM_THREADS; i++) {
			sq += terrs[i];
		}
		err = sqrt(sq / nRatings);
		//if (rank == 0 ) {
			printf("BIAS RMSE=%g time=%g\n", err, get_time()-start);
		//}
	}
	float sum = 0;
	for(int r = rank; r < nRatings; r+=NUM_THREADS) {
		ratings[r].cache = ratings[r].rating - mu - bi[ratings[r].item] - bu[ratings[r].user];
		sum += ratings[r].cache;
	}
	printf("Average normalized = %g\n", sum / (float)nRatings);
	for(int i = rank; i < nUsers*nFeatures;i+=NUM_THREADS) {
		p[i] =  randF(-1,1);//-.003, .001);
	}
	for(int i = rank; i < nItems*nFeatures;i+=NUM_THREADS) {
		q[i] = randF(-1,1); //.003, .001);
		x[i] = randF(-.01, .01);
		y[i] = randF(-.01, .01);
	}
}

float predict(int uid, int iid)
{
	float sum = mu + bu[uid] +bi[iid];
	for(int f = 0; f < nFeatures; f++) {
		sum += q[iid*nFeatures+f] * p[uid*nFeatures+f];
	}
	if (sum > 100/SCORENORM) sum = 100 /SCORENORM ;
	if (sum < 0) sum = 0;
	//printf("* %g %g \n", mu + bu[uid] + bi[iid], sum);

	return sum; // * 100;
}


float validate(bool print = false)
{
	int uid, n, i, iid, stmp, day, hr, min, sec;
	float  score;
	int didx = 0;
	unsigned int uidx=0, ridx=0, iidx=0;
	ssize_t size;
	char *data = (char *)open_read(VFILE, &size);

	int count = 0;
	float sq = 0, err;
	while(1) {
		if ( parseInt(data, &didx, &uid, size) ) {
			break;
		}
		//printf("' %c\n", data[didx-1]);
		uidx = umap[uid];
		parseInt(data, &didx, &n, size);
		for(i =0; i < n; i++) {
			parseInt(data, &didx, &iid, size);
			parseInt(data, &didx, &stmp, size);
			parseInt(data, &didx, &day, size);
			parseInt(data, &didx, &hr, size);
			parseInt(data, &didx, &min, size);
			parseInt(data, &didx, &sec, size);
			score = (float) stmp;
			score = score / SCORENORM;
			iidx = imap[iid];
			count ++;
			float pred = predict(uidx, iidx);
			err = score - pred;
			if(print)
				printf("%g %g %g\n", score, pred, err);
			sq += pow(err*SCORENORM, 2);
		}
	}
	munmap(data, size);
	return sqrt(sq / count);
}

void *train_model(void *ptr = NULL) {
	int rank = 0;
	float sum[nFeatures];
	if(ptr != NULL)  {
		rank = *(int *)ptr;
	}
	float lasterr = 1e6+1, sq = 0, err = 1e6;
	int epoch = 0;
	while (epoch < maxepochs) { // && (epochs < 10 || (lasterr - err) > 1e-6))
		lasterr = err;
		sq = 0;
		double start = get_time();

	//	pthread_barrier_wait(&barrier);
		for(int r = 0; r < nRatings; r++ ) {
			struct rating_s rating = ratings[r];
			float pred = mu + bi[rating.item] + bu[rating.user];
			for(int f1 = 0; f1 < nFeatures; f1++) {
				pred += p[rating.user*nFeatures + f1]*q[rating.item*nFeatures+f1];
			}
			err = rating.rating - pred;
			sq += err*err*SCORENORM*SCORENORM;


			for(int f = rank; f < nFeatures; f+=NUM_THREADS) {
				float tmpq = q[rating.item*nFeatures+f];
				float tmpp = p[rating.user*nFeatures+f];

				q[rating.item*nFeatures+f] += GAMMA*(err*tmpp - LAMBDA*tmpq);
				//p[rating.user*nFeatures+f] += GAMMA*(err*tmpq - LAMBDA*tmpp);
			}
			bu[rating.user] += GAMMA*(err - LAMBDA*bu[rating.user]);
			bi[rating.item] += GAMMA*(err - LAMBDA*bi[rating.item]);
		}
			/*int ridx = 0;
			for(int uidx = 0; uidx < nUsers; uidx ++) { //+=NUM_THREADS)
				struct user_s user = users[uidx];
				for(int r = 0; r < user.count; r++) {
					struct rating_s rating = ratings[ridx + r];
					float pred = 0; //mu + bi[rating.item] + bu[rating.user];
					for(int f1 = 0; f1 < f; f1++) {
						pred += p[rating.user*nFeatures + f1]*q[rating.item*nFeatures+f1];
					}
					//printf("%g\n",  pred);
					err = rating.rating - pred;
					sq += pow(err,2);

					//for(int f = 0; f < nFeatures; f++) {
					float tmpq = q[rating.item*nFeatures+f];
					float tmpp = p[rating.user*nFeatures+f];

					//sum[f] += err*tmpq;

					q[rating.item*nFeatures+f] += GAMMA*(err*tmpp - LAMBDA*tmpq);
					p[rating.user*nFeatures+f] += GAMMA*(err*tmpq - LAMBDA*tmpp);
					//}
				}

				for(int r = 0; r < user.count; r++) {
				  struct rating_s rating = ratings[ridx + r];
				  float rb = rating.cache;
				  for(int f = 0; f < nFeatures; f++) {
				  int i = rating.item + f;
				  x[i] += GAMMA*(invsqru*rb*sum[f] - LAMBDA*x[i]);
				  y[i] += GAMMA*(invsqru*sum[f] - LAMBDA*y[i]);
				  }
				  }
				ridx += user.count;
			}*/
		//pthread_barrier_wait(&barrier);
			err = sqrt(sq / nRatings);
			printf("epoch=%d RMSE=%g VMRSE=%g" , epoch++, err, validate());
			printf(" time=%g rank=%d\n", start-get_time(), rank);
	}
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
	float  score;
	int didx = 0;
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
			users[uidx].sum = 0;
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
			score = (float) stmp;
			score /= SCORENORM;
			//printf("%f\n", score);
			if( imap.count(iid) == 0) {
				iidx = imaplen;
				imaplen ++;
				imap[iid] = iidx;

				items[iidx].id = iid;
				items[iidx].count = 0;
				items[iidx].sum = 0;
			} else {
				iidx = imap[iid];
			}
			users[uidx].count += 1;
			users[uidx].sum += score;
			items[iidx].count += 1;
			items[iidx].sum += score;
			ratings[ridx].item = iidx;
			ratings[ridx].user = uidx;
			ratings[ridx].rating = score;
			mu += score;
			ridx++;
		}
	}
	munmap(data, size);
	nItems = imaplen;
	nUsers = umaplen;
	nRatings = ridx;
	printf("nRatings = %d\n", nRatings);
	mu /= (float)ridx;

	FILE *tmp = fopen("tmp/values", "w");
	fprintf(tmp, "%u %u %u\n", nRatings, nUsers, nItems);
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
	printf("%g %g %g %g\n", p[0], q[0], x[0], y[0]);
	printf("%g %g %g %g\n", p[1], q[1], x[1], y[1]);
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
	while ( (c = getopt(argc, argv, "itvme:")) != -1) {
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
		fscanf(tmp, "%u %u %u\n", &nRatings, &nUsers, &nItems);
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

	print_model_stats();

	if(isTrain) {
		kickoff(train_model);
	}

	print_model_stats();

	if (isVerify) {
		printf("VRMSE = %g\n", validate(true));
	}
	pthread_barrier_destroy(&barrier);
	pthread_exit(NULL);

	exit (0);
}
