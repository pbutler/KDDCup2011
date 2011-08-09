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
#include <pthread.h>
#include "barrier.h"
#include <assert.h>

#include "benchmark.h"
#include "data.h"
#include "allmodels.h"

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

//**START PARAMS
double decay = 0.7;
double decaypq = 0.9;
double decay2 = 0.8;
double userStep = 1.19006;
double itemStep = 0.0135704;
double genreStep = 0.0009;
double albumStep = .001;
double artistStep = 0.0468871;
double userReg = 1;
double itemReg = 1;
double genreReg = 1.;
double albumReg = .5;
double artistReg = 1.5;
double itemStep2 = .015;
double userStep2 = .06;
double genreStep2 = .001;
double albumStep2 = .003;
double artistStep2 = .03;
double pStep = .02;
double qStep =.001;
double pReg = 1;
double qReg = 1;
double pArStep = .006;
double qArStep = .001;
double pArReg = .5;
double qArReg = 1.2;
double pAlStep = .001;
double qAlStep = .02;
double pAlReg = 1;
double qAlReg = 1;
double pgStep = .003;
double qgStep = .001;
double pgReg = 0.5;
double qgReg = 0.5;
//**END PARAMS

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


#define NUM_THREADS 1
//#define SCORENORM  1.f

int maxepochs = 20;
int maxepochsbias = 20;
int maxfaults = 3;

const unsigned int nFeatures = 100;
char pfile[256];

pthread_mutex_t mutexB = PTHREAD_MUTEX_INITIALIZER;
int curItems[NUM_THREADS];
pthread_mutex_t mutexFeature[nFeatures];

double terrs[NUM_THREADS];

double *bu, *bi, *p, *x, *y, *q, *bg, *bal, *bar, *bit;
double *pAl, *pAr, *qAl, *qAr, *pg, *qg;

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


inline double randF(double min, double max)
{
   double a =  (double)random() /((double)RAND_MAX +1);
   return a * (max - min) + min;
}


void load_model()
{
	bal = (double*) open_rw("bal.mmap", sizeof(double)*nAlbums);
	bar = (double*) open_rw("bar.mmap", sizeof(double)*nArtists);
	bg = (double*) open_rw("bg.mmap", sizeof(double)*nGenres);
	bi = (double*) open_rw("bi.mmap", sizeof(double)*nItems);
	bit = (double*) open_rw("bit.mmap", sizeof(double)*nItems*NBINIT);
	bu = (double*)open_rw("bu.mmap", sizeof(double)*nUsers);
	p = (double *)open_rw("p.mmap", sizeof(double)*nFeatures*nUsers);
	q = (double*)open_rw("q.mmap", sizeof(double)*nFeatures*nItems, MADV_RANDOM);
	x = (double*)open_rw("x.mmap", sizeof(double)*nFeatures*nItems, MADV_RANDOM);
	y = (double*)open_rw("y.mmap", sizeof(double)*nFeatures*nItems, MADV_RANDOM);

	pg = (double *)open_rw("pG.mmap", sizeof(double)*nFeatures*nUsers);
	qg = (double*)open_rw("qG.mmap", sizeof(double)*nFeatures*nGenres, MADV_RANDOM);
	pAl = (double *)open_rw("pAl.mmap", sizeof(double)*nFeatures*nUsers);
	qAl = (double*)open_rw("qAl.mmap", sizeof(double)*nFeatures*nAlbums, MADV_RANDOM);
	pAr = (double *)open_rw("pAr.mmap", sizeof(double)*nFeatures*nUsers);
	qAr = (double*)open_rw("qAr.mmap", sizeof(double)*nFeatures*nArtists, MADV_RANDOM);
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
	for(int i =rank; i < nUsers*NBINIT; i += NUM_THREADS) {
	       bit[i] = 0;
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
				double tmpbit = bit[rating.item*NBINIT+ITBIN(rating.day)];
#ifdef BIASES
				pred = mu + bu[u] + tmpbi + tmpbit;
#else
				pred = 0;
#endif
				//fprintf(stderr, "%d\n", rating.item);
#ifdef BIASESG
				pair<multimapII::const_iterator, multimapII::const_iterator> p =
					genreItemMap.equal_range(rating.item);
				double ag = 0;
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					ag += bg[(*i).second];
				}
				pred += ag;// / (double) ng;
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
			       	bit[rating.item*NBINIT+ITBIN(rating.day)] += itemStep*(err-itemReg*tmpbit);
#endif // BIASES
#ifdef BIASESG
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					int gid = (*i).second;
					bg[gid] += genreStep*(err - genreReg*bg[gid]); /// (double) ng);
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
				pred = mu + bu[u] + bi[rating.item] + bit[rating.item*NBINIT+ITBIN(rating.day)];
#else
				pred = 0;
#endif //BIASES
				pair<multimapII::const_iterator, multimapII::const_iterator> p =
					genreItemMap.equal_range(rating.item);
				//printf("%g %g %g %d ", mu, bu[u], bi[rating.item],rating.item);
#ifdef BIASESG
				double ag = 0;
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					int gid = (*i).second;
					ag += bg[gid];
				}
				pred += ag;// / (double)ng;
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
#if defined(LATENT) || defined(LATENTAR) || defined(LATENTAL)
	double sqnF = 1./sqrt(nFeatures);
	for(int i = rank; i < nUsers*nFeatures;i += NUM_THREADS) {
		p[i] = randF(pMin, pMax) * sqnF;
		pAl[i] = randF(pMin, pMax) * sqnF;
		pAr[i] = randF(pMin, pMax) * sqnF;
		pg[i] = randF(pMin, pMax) * sqnF;
	}

	for(int i = rank; i < nGenres*nFeatures;i+=NUM_THREADS) {
		qg[i] = randF(qMin, qMax) * sqnF;
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

inline double predict(int uid, int iid, int day, bool print/* = false*/)
{
	struct item_s item = items[iid];
	double sum = 0;
#if defined(BIASESG) || defined(LATENTG)
#endif
	if (print)
		printf("\n\n");
#ifdef BIASES
	sum += mu + bu[uid] + bi[iid] + bit[iid*NBINIT+ITBIN(day)];
	if(print)
		printf("bu/i: %g %g %g\n", mu, bu[uid], bi[iid]);
#endif //BIASES
	pair<multimapII::const_iterator, multimapII::const_iterator> gp =
		genreItemMap.equal_range(iid);
#ifdef BIASESG
	bool goprint = false;
	double ag = 0;
	for (multimapII::const_iterator i = gp.first; i != gp.second; ++i) {
		int gid = (*i).second;
		if (!goprint && print) {
			printf("bg: ");
		}
		goprint = true;
		ag += bg[gid];
		if(print)
			printf("%g ", bg[gid]);
	}
	sum += ag; // /(double)ng;
	if(print && goprint)
		printf("\n");
#endif //BIASESG
	double apq = 0;
#ifdef LATENT
	for(int f = 0; f < nFeatures; f++) {
		/*if (print )
			printf("pq: %g %g %g\n",
				       	p[uid*nFeatures+f],
					q[iid*nFeatures+f],
					q[iid*nFeatures+f] * p[uid*nFeatures+f]);
					*/
		apq += CLAMP2(q[iid*nFeatures+f] * p[uid*nFeatures+f]);
	}
	sum += apq;
	if(print)
		printf("pqT: %g\n", apq);
#endif //LATENT
#ifdef LATENTG
	apq = 0;
	for (multimapII::const_iterator i = gp.first; i != gp.second; ++i) {
		int gid = (*i).second;
		for(int f= 0; f < nFeatures; f++) {
			apq += pg[uid*nFeatures + f]*qg[gid*nFeatures + f];
		}
	}
	if(print)
		printf("Gpq: %g\n", apq);
	sum += apq; // / ng;
#endif //LATENTG
#ifdef LATENTAL
	apq = 0;
	if(item.albumid > -1) {
		sum += bal[item.albumid];
		for(int f = 0; f < nFeatures; f++) {
			apq += CLAMP2(qAl[item.albumid*nFeatures+f]*pAl[uid*nFeatures+f]);
		}
	}
	sum += apq;
	if(print)
		printf("Alpq: %g\n", apq);
#endif //LATENTAL
#ifdef LATENTAR
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
#endif //LATENTAL
	if(print)
		printf("%g --> ", sum);
	return CLAMP(sum);
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
				double err = (double) rating.rating - predict(u, rating.item, rating.day);
				double tmpbi = bi[rating.item];
				double tmpbit = bit[rating.item*NBINIT+ITBIN(rating.day)];
				if (err > 100 || err < -100) {
					printf("ERR OOB: %g %g %g\n", err, (double)rating.rating, predict(u, rating.item, rating.day));
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
				bit[rating.item*NBINIT+ITBIN(rating.day)] += itemStep2*(err - itemReg*tmpbit);
#endif //BIASES

				pair<multimapII::const_iterator, multimapII::const_iterator> p =
					genreItemMap.equal_range(rating.item);
				for (multimapII::const_iterator i = p.first; i != p.second; ++i) {
					int g = (*i).second;
#ifdef BIASESG
					bg[g] += genreStep2*(err - genreReg*bg[g]); /// ng);
#endif //BIASESG
#ifdef LATENTG
					for(int f = 0; f < nFeatures; f++) {
						int iif = g*nFeatures+f;
						double tmpq = qg[iif];
						double tmpp = pg[uf+f];
						qg[iif] += qgStep*(err*tmpp - qgReg*tmpq); // /ng);
						pg[uf+f] += pgStep*(err*tmpq - pgReg*tmpp); // /ng);
					}
#endif //BIASESG
				}
				if(item.albumid > -1) {
#ifdef BIASESA
					bal[item.albumid] += albumStep2*(err - albumReg*bal[item.albumid]);
#endif //BIASES A
#ifdef LATENTAL
					for(int f = 0; f < nFeatures; f++) {
						int iif = item.albumid*nFeatures+f;
						double tmpq = qAl[iif];
						double tmpp = pAl[uf+f];
						qAl[iif]+= qAlStep*(err*tmpp - qAlReg*tmpq);
						pAl[uf+f]+= pAlStep*(err*tmpq - pAlReg*tmpp);
					}
#endif // LATENTA
				}
				if(item.artistid > -1) {
#ifdef BIASESA
					bar[item.artistid] += artistStep2*(err - artistReg*bar[item.artistid]);
#endif //BIAESSA
#ifdef LATENTAR
					for(int f = 0; f < nFeatures; f++) {
						int iif = item.artistid*nFeatures+f;
						double tmpq = qAr[iif];
						double tmpp = pAr[uf+f];
						qAr[iif] += qArStep*(err*tmpp - qArReg*tmpq);
						pAr[uf+f] += pArStep*(err*tmpq - pArReg*tmpp);
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

void read_params(void)
{
	FILE *fp = fopen(pfile, "r");
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
		READSET(genreStep2);
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

		READSET(pAlStep);
		READSET(pAlReg);
		READSET(qAlStep);
		READSET(qAlReg);

		READSET(pArStep);
		READSET(pArReg);
		READSET(qArStep);
		READSET(qArReg);

		READSET(pgStep);
		READSET(pgReg);
		READSET(qgStep);
		READSET(qgReg);
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

void save_model_results_local(FILE *file)
{
#define WRITEPD(name) fprintf(file, #name "=%lg\n", name);
	WRITEPD(decay);
	WRITEPD(decaypq);
	WRITEPD(decay2);
	WRITEPD(userStep);
	WRITEPD(itemStep);
	WRITEPD(genreStep);
	WRITEPD(albumStep);
	WRITEPD(artistStep);
	WRITEPD(userReg);
	WRITEPD(itemReg);
	WRITEPD(genreReg);
	WRITEPD(albumReg);
	WRITEPD(artistReg);
	WRITEPD(itemStep2);
	WRITEPD(userStep2);
	WRITEPD(genreStep2);
	WRITEPD(albumStep2);
	WRITEPD(artistStep2);
	WRITEPD(pStep);
	WRITEPD(qStep);
	WRITEPD(pReg);
	WRITEPD(qReg);
	WRITEPD(pArStep);
	WRITEPD(qArStep);
	WRITEPD(pArReg);
	WRITEPD(qArReg);
	WRITEPD(pAlStep);
	WRITEPD(qAlStep);
	WRITEPD(pAlReg);
	WRITEPD(qAlReg);
	WRITEPD(pgStep);
	WRITEPD(qgStep);
	WRITEPD(pgReg);
	WRITEPD(qgReg);

	WRITEPD(xStep);
	WRITEPD(xReg);
	WRITEPD(yStep);
	WRITEPD(yReg);

	WRITEPD(pMax);
	WRITEPD(qMax);
	WRITEPD(xMax);
	WRITEPD(yMax);

	WRITEPD(pMin);
	WRITEPD(qMin);
	WRITEPD(xMin);
	WRITEPD(yMin);

#define WRITEPI(name) fprintf(file, #name "=%d\n", name);
	fprintf(file, "seed=%ld\n", SEED);
	WRITEPI(nFeatures);
	WRITEPI(NUM_THREADS);
	WRITEPD(SCORENORM);
	WRITEPI(maxepochs);
	WRITEPI(maxepochsbias);
	WRITEPI(maxfaults);

	WRITEPI(BIASES);
	WRITEPI(BIASESG);
	WRITEPI(BIASESA);
	WRITEPI(LATENT);
	WRITEPI(LATENTAL);
	WRITEPI(LATENTAR);
	WRITEPI(LATENTG);
	WRITEPI(NBINIT);

}

int main (int argc, char **argv)
{
	int c;
	bool isInit = false;
	bool isTrain = false;
	bool isVerify = false;
	bool isBuildModel = false;
	bool isStats = false;
	bool isPredict = false;
	bool printVerify = true;
	char modelname[256]="";

	while ( (c = getopt(argc, argv, "itvVme:spf:bE:hP:T:S:r")) != -1) {
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
			case 'S':
				strcpy(modelname, optarg);
				break;
			case 'T':
				set_tmp_dir(optarg);
				break;
			case 'f':
				maxfaults = atoi(optarg);
				break;
			case 'p':
				isPredict = true;
				break;
			case 'P':
				strcpy(pfile, optarg);
				read_params();
				break;
			case 'h':
				print_usage(argv[0]);
				exit(0);
				break;
			case 'r':
				randomize();
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

	for(int i = 0; i < nFeatures; i++) {
		pthread_mutex_init(&mutexFeature[i], NULL);
	}

	load_data(isInit);
	load_model();

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
	if(strlen(modelname) > 0) {
		save_model_results(modelname);
	}
	pthread_barrier_destroy(&barrier);
	pthread_exit(NULL);

	exit (0);
}


