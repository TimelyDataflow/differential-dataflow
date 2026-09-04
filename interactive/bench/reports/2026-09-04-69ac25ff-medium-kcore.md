### initial epoch (load + first computation)

| workload | corgi w1 | vec w1 | corgi w4 | vec w4 | vec/corgi w1 | vec/corgi w4 |
|---|---|---|---|---|---|---|
| kcore | 280.2ms (n=3) | 656.4ms (n=3) | 88.1ms (n=3) | 241.2ms (n=3) | 2.34x | 2.74x |

### per churn epoch (median over the run's rounds)

| workload | corgi w1 | vec w1 | corgi w4 | vec w4 | vec/corgi w1 | vec/corgi w4 |
|---|---|---|---|---|---|---|
| kcore | 2.3ms (n=3) | 5.3ms (n=3) | 899µs (n=3) | 1.9ms (n=3) | 2.32x | 2.11x |
