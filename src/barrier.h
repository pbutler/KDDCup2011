#ifndef __BARRIER_H__
#define __BARRIER_H__

#if !defined(WITH_BARRIER) || defined(CYGWIN)
#define pthread_barrier_t barrier_t
#define pthread_barrier_attr_t barrier_attr_t
#define pthread_barrier_init(b,a,n) barrier_init(b,n)
#define pthread_barrier_destroy(b) barrier_destroy(b)
#define pthread_barrier_wait(b) barrier_wait(b)
#endif


typedef struct {
	int needed;
	int called;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
} barrier_t;


int barrier_init(barrier_t *barrier,int needed);
int barrier_destroy(barrier_t *barrier);
int barrier_wait(barrier_t *barrier);


int barrier_init(barrier_t *barrier,int needed)
{
	barrier->needed = needed;
	barrier->called = 0;
	pthread_mutex_init(&barrier->mutex,NULL);
	pthread_cond_init(&barrier->cond,NULL);
	return 0;
}


int barrier_destroy(barrier_t *barrier)
{
	pthread_mutex_destroy(&barrier->mutex);
	pthread_cond_destroy(&barrier->cond);
	return 0;
}

int barrier_wait(barrier_t *barrier)
{
	pthread_mutex_lock(&barrier->mutex);
	barrier->called++;
	if (barrier->called == barrier->needed) {
		barrier->called = 0;
		pthread_cond_broadcast(&barrier->cond);
	} else {
		pthread_cond_wait(&barrier->cond,&barrier->mutex);
	}
	pthread_mutex_unlock(&barrier->mutex);
	return 0;
}

#endif
