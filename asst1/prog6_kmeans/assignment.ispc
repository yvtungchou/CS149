static inline double dist(double *x, double *y, int nDim, int nPts) {
    double accum = 0.0;
    for (int i = 0; i < nDim; i++) {
        double diff = x[i * nPts] - y[i];
        accum += diff * diff;
    }
    return sqrt(accum);
}

export void assignment_ispc(uniform int M, uniform int start, 
                            uniform int end, uniform int N,
                            uniform double data[], uniform double clusterCentroids[],
                            uniform double minDist[], uniform int clusterAssignments[])
{
    foreach (m = 0 ... M) { 
        for (uniform int k = start; k < end; k++) { // maybe uniform int?
            double d = dist(&data[m],
                            &clusterCentroids[k * N], N, M);
            if (d < minDist[m]) {
                minDist[m] = d;
                clusterAssignments[m] = k;
            }
        }
    }
}

task void assignment_ispc_task(uniform int M, uniform int start, 
                                uniform int end, uniform int N, uniform int worksPerTask, 
                                uniform double data[], uniform double clusterCentroids[],
                                uniform double minDist[], uniform int clusterAssignments[])
{

    // taskIndex is an ISPC built-in
    
    uniform int mStart = taskIndex * worksPerTask;
    uniform int mStop = mStart + worksPerTask;
    
    foreach (m = mStart ... mStop) { 
        // print("programIndex: %\n", programIndex);
        for (uniform int k = start; k < end; k++) { 
            double d = dist(&data[m],
                            &clusterCentroids[k * N], N, M);
            if (d < minDist[m]) {
                minDist[m] = d;
                clusterAssignments[m] = k;
            }
        }
        // here update cluster info
    }
}

export void assignment_ispc_withtasks(uniform int M, uniform int start, 
                                    uniform int end, uniform int N,
                                    uniform double data[], uniform double clusterCentroids[],
                                    uniform double minDist[], uniform int clusterAssignments[])
{

    uniform int worksPerTask = M / 64; // create 64 tasks

    launch[M / worksPerTask] assignment_ispc_task(M, start, end, N, worksPerTask, 
                                    data, clusterCentroids, minDist, 
                                    clusterAssignments);
}


export void sum_cost_ispc(uniform int M, uniform int N, uniform int K, uniform double accum[], 
                            uniform double data[], uniform double clusterCentroids[],
                            uniform int clusterAssignments[])
{
    uniform double * local_accum = uniform new double[K]; // avoid this confusing dynamic allcoation
    foreach (m = 0 ... M) {
        int k = clusterAssignments[m]; // could avoid this k
        // accum[k] += dist(&args->data[m * args->N], &args->clusterCentroids[k * args->N], args->N);
        local_accum[k] += dist(&data[m], &clusterCentroids[k * N], N, M);
    }
    
    for (uniform int k = 0; k < K; k++) {
        accum[k] = reduce_add(local_accum[k]);
    }
    
}