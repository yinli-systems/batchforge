# Preregistered QA answer-contract repair

The complete 10,570-question SQuAD baseline remains a failed quality gate:
EM 53.8221%, F1 75.8213%. The public corpus is not a hidden test. This successor
changes answer formatting, not model weights, gold references or scoring gates.

Candidate `json-span-v1` supplies two fixed synthetic examples and requests one
JSON string field `answer`, copied literally from the passage. The evaluator
accepts only that exact schema and a literal passage substring. Invalid JSON,
duplicate fields, explanation, empty or invented spans score zero; no repair,
gold-dependent extraction or alternative selection is allowed. The original
response bytes remain the Batch API result. Extracted answers and raw responses
have separate hashes. Official SQuAD normalization and maximum-over-references
EM/F1 remain unchanged. Temperature 0, natural EOS and 128 output tokens remain.

Before inference, pin dataset SHA, code commit, runtime fingerprint, selected
ID hashes, workload hashes and the original failed baseline score SHA. Choose
100 calibration IDs with seed 202610251, then 1,000 holdout IDs with seed
202610252 excluding all calibration IDs. Both are sampled from the same pinned
public SQuAD dev corpus. Compare each cohort to its matching existing baseline
IDs; do not compare a selected subset to the full-dataset mean.

Run calibration through the real private Batch API only after the prior fixed
GPU campaign has closed and service identities, queue and readiness are checked.
The unchanged calibration thresholds are EM >=70%, F1 >=80%, full delivery
within 1,200 seconds. Run the frozen 1,000-task holdout only if calibration meets
both thresholds, with no prompt/parser edits in between. Holdout qualification
requires complete delivery, the same 70%/80% thresholds and a 1,200-second bound.
Report invalid contracts, paired wins/losses, usage, full-cohort wall time and
operator-confirmed $16.10/h allocation separately from invoice cost.

A passing sampled holdout does **not** replace the failed complete-dev result.
No claim of full-dataset qualification, unbiased hidden-test generalization or
production SLA follows. A new full-cohort experiment needs its own declared
scope and storage capacity. Do not raise the 50,000 retained-task quota, add a
tenant to evade it, delete history or change retention. The current 46,917
retained tasks leave 3,083 available; this plan accepts at most 1,100 new tasks.
No new GPU, paid resource or paid inference API is used.
