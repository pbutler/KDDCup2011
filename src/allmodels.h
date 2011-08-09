#ifndef ALLMODELS_H
#define ALLMODELS_H

#include <stdlib.h>
#include <stdio.h>

extern long SEED;
void randomize();
//this must be defined per model
inline double predict(int uid, int iid, int day, bool print = false);

void save_model_results_local(FILE *f);

//these are general functions:w
//
double validate(bool print = false);
void make_predictions();
void save_model_results(const char *modelname);
#endif //ALMODELS_H
