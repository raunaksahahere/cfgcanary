# 0004. Gate: three-zone decision with predeclared looks and instance-clustered variance

**Status:** accepted (revised before v0.1.0, see "Revision")

**Context.** The design brief (P2, p. 34) asks for "sequential tests (SPRT) or two-proportion
tests" and names small canary cohorts as the hard part. Repeatedly applying a fixed-sample test
inflates the false-positive rate, and "not significantly worse" is not evidence of safety.

**Decision.**
- Each metric is a proportion: failed requests, requests slower than a histogram bucket boundary,
  instances that restarted. Latency is compared as the *fraction over a bucket boundary*, never as a
  ratio of interpolated quantiles.
- Two one-sided tests per metric and look, both against the configured absolute **margin** (the
  tolerated degradation):
  - **harm**: H0 "p_c − p_k ≤ margin" (≤ 0 for guard metrics); rejecting it means degradation
    beyond the tolerance → FAIL;
  - **non-inferiority**: H0 "p_c − p_k ≥ margin"; rejecting it for every evidence metric → PASS.
  Otherwise INCONCLUSIVE. This is the three-zone (equivalence-style) decision.
- Variance is the larger of the binomial variance and a between-instance (cluster-robust) estimate,
  with Student-t critical values on the instance degrees of freedom, because requests from one
  instance are correlated. At the null boundary the binomial part uses the Farrington–Manning
  restricted maximum-likelihood estimate (verified against a likelihood grid search in
  `internal/gate` tests), which stays positive when both cohorts have zero events.
- The policy declares `K` looks. Alpha is split by Bonferroni across looks, and for the harm test
  also across metrics. Promotion needs every evidence metric to pass non-inferiority at the same
  look (intersection–union, no further correction). After look `K` without a decision the release
  halts.
- **Guard metrics** (rare events such as restarts, where small cohorts cannot show safety) can
  fail a release but are not required to show non-inferiority. At least one metric must not be a
  guard.
- **Hard limits** (`hard_max`) roll back on an absolute proportion regardless of control.

**Why not a Wald SPRT.** The conditional rate-ratio SPRT gets no information when neither cohort
has errors, the normal healthy case, so it cannot promote. A margin-based test can.

**Revision.** The first implementation tested harm against zero. The power simulation
(`bench/gate_test.go`) showed that with 50 or 500 canaries and enough traffic it rolled back
79–100% of releases whose degradation was well *inside* the margin (+0.5 points against a 2-point
margin), which makes the margin meaningless at scale and would cause spurious rollbacks. Testing
harm against the margin fixed it: inside-margin changes promote, at-margin changes promote at
2–6% (the 5% error budget, within Monte Carlo error), +5-point changes roll back 100% from 50
canaries up.

**Consequences.** Conservative (Bonferroni wastes some power). With 5 heterogeneous canaries, or
about 20 requests per instance per look, the gate mostly halts: it never promotes harm, but it
cannot produce evidence, so size cohorts for power and use `hard_max` for severe failures.
Assumptions: requests within an instance are independent given its rate, no time trends;
correlated incidents across instances are not modelled. Results: `bench/results/gate_power.json`.
