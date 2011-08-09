#include "allmodels.h"
#include "data.h"
#include <math.h>
#include <time.h>

long SEED = -1;
void randomize()
{
	FILE *r = fopen("/dev/random", "r");
	fread(&SEED, sizeof(SEED), 1, r);
	fclose(r);
	srandom(SEED);
}

void save_model_results(const char *modelname)
{
	char filename[512];
	strcpy(filename, modelname);
	strcat(filename, ".model");
	ssize_t total_ratings = nRatings + (10*nUsers);
	printf("Saving model to %s\n", filename);
	double *results = (double *)open_rw(filename, total_ratings*sizeof(double));
	ssize_t i = 0;
	for(int u = 0; u < nUsers; u++) {
		for(int r = STARTRATING(u); r < ENDRATING(u); r++) {
			struct rating_s rating = ratings[r];
			results[i++] = predict(u, rating.item, rating.day);
		}
	}
	for(int u = 0; u < nUsers; u++) {
		for(int r = u*4; r < (u+1)*4; r++) {
			struct rating_s rating = validations[r];
			results[i++] = predict(u, rating.item, rating.day);
		}
	}
	for(int u = 0; u < nUsers; u++) {
		for(int r = u*6; r < (u+1)*6; r++) {
			struct rating_s rating = tests[r];
			results[i++] = predict(u, rating.item, rating.day);
		}
	}
	munmap(results, total_ratings*sizeof(double));
	char psfile[512];
	strcpy(psfile, modelname);
	strcat(psfile,".params");
	FILE *f = fopen(psfile, "w");
	save_model_results_local(f);
	fclose(f);

}

double validate(bool print/* = false*/)
{
	double sq = 0;
	for(int u = 0; u < nUsers; u++) {
		for(int r = u*4; r < (u+1)*4; r++) {
			double pred = predict(u, validations[r].item, validations[r].day, print);
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
                        double pred = predict(u,tests[iid].item, tests[iid].day);
                        fprintf(fp, "%lf\n", pred*SCORENORM);
                }
        }
        fclose(fp);
}

