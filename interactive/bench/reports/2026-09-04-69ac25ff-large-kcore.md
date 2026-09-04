### initial epoch (load + first computation)

| workload | corgi w1 | vec w1 | corgi w4 | vec w4 | vec/corgi w1 | vec/corgi w4 |
|---|---|---|---|---|---|---|
| kcore | 4.55s (n=2) | 8.23s (n=2) | 1.43s (n=2) | 2.27s (n=2) | 1.81x | 1.59x |

### per churn epoch (median over the run's rounds)

| workload | corgi w1 | vec w1 | corgi w4 | vec w4 | vec/corgi w1 | vec/corgi w4 |
|---|---|---|---|---|---|---|
| kcore | 25.9ms (n=2) | 53.3ms (n=2) | 9.5ms (n=2) | 19.5ms (n=2) | 2.06x | 2.06x |
