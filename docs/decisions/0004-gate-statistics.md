# 0004. Gate: predeclared looks, non-inferiority plus harm tests, instance-clustered variance

**Status:** accepted

**Context.** P2 (p. 34) asks for "sequential tests (SPRT) or two-proportion tests" and notes that
small canary cohorts are the hard part. Repeatedly applying a fixed-sample test inflates the
false-positive rate, and "not significantly worse" is not evidence of safety.

**Decision.**
- Each metric is a proportion: failed requests, requests slower than a histogram bucket boundary,
  instances that restarted. Latency is compared as the *fraction over a bucket boundary*, never as
  a ratio of interpolated quantiles.
- Two one-sided tests per metric and look: a **harm** test (H0: canary no worse than control) and a
  **non-inferiority** test with an absolute margin (H0: canary worse than control by at least the
  margin; Farrington-Manning restricted variance, which stays defined when both arms have zero
  events).
- The variance is the larger of the binomial variance and a between-instance (cluster-robust)
  estimate, with Student-t critical values on the instance degrees of freedom, because requests
  from one instance are correlated.
- The policy declares `K` looks. Alpha is split by Bonferroni across looks, and for the harm test
  also across metrics. Promotion needs every metric to pass non-inferiority at the same look.
  After look `K` without a decision the release halts.

**Why not a Wald SPRT.** The conditional rate-ratio SPRT gets no information when neither cohort
has errors, which is the normal healthy case, so it cannot promote. A margin-based test can.

**Consequences.** Conservative: Bonferroni wastes some power. Simulations for 5, 50 and 500 canary
instances are in `bench/results/gate-power.json`. Limits are listed in the design guide.
