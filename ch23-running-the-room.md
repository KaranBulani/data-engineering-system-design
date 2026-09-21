# Chapter 23 — Running the Room

> Part VIII — Interview Execution

Everything before this chapter is what you know. This chapter is how you
*perform* it: how to drive a 45-minute design conversation so that the
interviewer sees a senior engineer thinking, not a candidate reciting. The
difference is entirely in the mechanics of running the room.

## The Question

*"How do I structure 45 minutes so the requirements get asked, the right
decisions get made with visible trade-offs, the failure modes get named
before I'm asked — and I still finish?"*

## The Physics

### The time budget

| Minutes | Activity | Output |
|---|---|---|
| 0-5 | Requirements extraction (ch02 script) + assumptions stated aloud | written requirements the interviewer agreed to |
| 5-10 | High-level flow: sources -> planes -> processing -> serving | the skeleton diagram |
| 10-30 | 2-3 deep-dives at the cascade levels the problem stresses | decisions with trade-offs, named failure modes |
| 30-40 | The -ilities: failure/recovery, security, cost, ops (ch20-22) | the "and it survives reality" pass |
| 40-45 | Wrap: recap decisions, state what would change them | the senior close |

The deep-dive selection *is* the skill: a fraud-detection prompt deserves
event-time/watermarks/state (ch08) and serving KV (ch16); a BI-platform
prompt deserves batch incrementality (ch07), lakehouse (ch12), and workload
management (ch16). Spend minutes where the problem's difficulty lives.

### How to present a decision — the four-beat pattern

Every architectural choice, the same rhythm:

1. **Options**: "For X, the realistic options are A and B."
2. **Trade-offs**: "A gives latency at cost/complexity; B gives simplicity
   at a latency floor."
3. **Call**: "Given the requirements we agreed — [requirement] — I'd take
   A."
4. **What would change it**: "If the SLA were 5 minutes instead of 5
   seconds, B wins and the design simplifies to..."

Beat 4 is the senior marker. It shows the decision is *conditional on
requirements*, not on taste — and it hands the interviewer a lever to test
you with, which they will use, which you will then pass.

### Proactive failure modes — the senior signal

Name them before being asked: "Two things break in this design: first,
consumer lag past retention is silent loss, so I'd alert on lag-vs-retention
headroom; second, the speed layer's approximate views disagree with batch in
the overlap window, so I'd reconcile nightly." This does three things:
proves operational experience, preempts the probe, and demonstrates the
correctness mindset. An interviewer with nothing left to probe moves to
*depth* — which is where you win.

### Handling ambiguity

Vague prompt, evasive interviewer: **propose two interpretations, pick one,
note the other.** "This could mean per-order fraud decisions in-flight, or
post-hoc fraud analytics for review queues. I'll design the first — it's
the harder latency problem — and note where the second reuses this design."
This converts ambiguity from a stall into a display of judgment. Never
design for the union of all interpretations.

### Handling pushback

The pattern that wins: **"You're right — and here's the trade-off I was
making."** Concede genuine points (the interviewer often knows the system's
warts); defend conditional choices by *re-anchoring on requirements*, never
on preference. The losing patterns: defensiveness (ego read), instant
capitulation (no-confidence read), and name-dropping a vendor to resolve a
physics question (junior read). If pushback introduces a *fact* you missed,
absorb it visibly: "That changes the watermark decision — with 2-day-late
events, I'd need allowed-lateness plus a batch correction path, which pushes
me toward the lakehouse pattern."

### What the classic probes are really testing

| Probe | Actually testing |
|---|---|
| "How do you handle late data?" | event-time thinking (ch08) — do you reach for watermarks or hand-wave |
| "What if the table is 10x bigger?" | partition/scale instincts (ch07/19) — pruning, file sizing, compaction |
| "How do analysts query this?" | serving-layer awareness (ch16) — did you design the last mile or stop at processing |
| "What if a message is duplicated?" | idempotency reflex (ch05/08) — effectively-once as a design |
| "How does this recover?" | replay/backfill thinking (ch03/06/07/11) — is raw your insurance |
| "Isn't this over-engineered?" | ops-maturity governor (ch02) — can you defend *simplicity* |
| "How do you secure it?" | volunteered vs dragged (ch20) — TLS/least-privilege reflexes |

Read that table as a map of the previous 21 chapters compressed into
questions. Every probe is a chapter you already own.

### The trap list

1. **Jumping to tools** (ch01's anti-pattern, now an exam failure): naming
   Kafka in minute two without a latency requirement on the board.
2. **No explicit trade-offs**: presenting a design as inevitable rather
   than chosen — invites the "why not X?" you can't answer.
3. **Designing for ungiven scale**: 10 events/day with Kappa replay
   infrastructure; the interviewer hears cost-insensitivity (ch02).
4. **The infinite requirements loop**: 15 minutes of questions, no
   architecture. Five minutes, assumptions stated, draw (ch02).
5. **Ignoring cost and ops**: the -ilities pass skipped; senior interviewers
  *always* probe there (ch21-22).
6. **The monologue**: 20 minutes of uninterrupted diagram narration. The
   room should be a dialogue — check in at each beat: "does that match
   your mental model?"
7. **Finishing without the close**: no recap, no conditions — the interview
   ends on fatigue instead of judgment.

### Whiteboard/screen mechanics

- **Draw left-to-right: sources -> ingestion -> processing -> storage ->
  serving -> consumers.** The cascade as a picture (ch01).
- **Write requirements and assumptions in a corner and keep them visible** —
  every decision points back at one (ch02).
- **Name every box with its *role*** ("stream plane", "raw archive", "serving
  KV") and only *then* the technology — role first, product second, always.
- **Leave the diagram intact**; annotate trade-offs beside decisions rather
  than erasing history. The trade-off annotations *are* your interview
  artifact.

## The Options

There is no options table for running the room — but there are three
recognizable candidate modes:

| Mode | What it looks like | Read |
|---|---|---|
| The reciter | tool-first, no requirements, monologue | junior regardless of knowledge |
| The conversationalist | requirements -> options -> calls -> conditions | senior; invites depth probes and passes them |
| The perfectionist | all 21 chapters on one board, overtime, no close | knowledgeable but unpracticed — simulate to fix |

## Decision Rules

- **Spend the first five minutes on requirements and say them back** — the
  contract everything else cites.
- **Deep-dive where the problem stresses; skim where it doesn't.** Two or
  three levels, not all of them (ch01's half-life budget).
- **Four beats per decision: options, trade-offs, call, what-changes-it.**
- **Volunteer two failure modes per major component.** Preempt the probe.
- **Ambiguity -> two interpretations, design one, note the other.**
- **Pushback -> concede or re-anchor on requirements; never defend taste.**
- **Close the interview: recap the decisions as a chain and the conditions
  that would change them.** The last 60 seconds are the loudest.

## Failure Modes

- **The tool opening**: "I'd use Kafka and Flink" as sentence one — the
  interview is now a trivia contest you didn't set the rules for.
- **The requirements hostage situation**: refusing to draw until every
  requirement is answered — you've made the interviewer the blocker.
- **Trade-off amnesia under pressure**: the design is right but "why?"
  gets a hedge. Fix: rehearse beat 4 (what-would-change-it) for every major
  choice — it *is* the why.
- **The unwatched clock**: 35 minutes on ingestion details, serving and ops
  never reached — the design is unfinished by *presentation*, not by
  knowledge.
- **Defensive spiral**: one pushback met with resistance, the interviewer
  probes harder, the room turns adversarial. The concede-and-re-anchor
  pattern is the escape hatch, and it's learnable.
- **No close**: ends mid-diagram. The recap close is the highest
  ROI 60 seconds available.

## Interview Narration

(This chapter *is* narration, so instead — the opening 90 seconds,
transcribed, as the template:)

"Great — before I draw anything, let me pin down requirements, because
they'll drive every choice downstream. The two that matter most: latency —
from an event happening to someone acting on it, is this seconds, minutes,
or next-morning? — and correctness: is approximately-right-now then
exactly-right-later acceptable, or do we need exact immediately? Then
volume and growth, and whether there's PII in scope. I'll write these in
the corner and every decision will point back at one.

I'll assume [assumption] unless you tell me otherwise. Once we agree, I'll
draw the high-level flow left to right — sources, ingestion, processing,
storage, serving — and then I'd like to spend our deep-dive time on [the
stressed area], because that's where this problem's difficulty lives. And
I'll name failure modes as we go — two per component — rather than saving
them for the end. Sound like a plan?"

That's 90 seconds: requirements, contract, plan, collaboration. The
interviewer now knows the next 40 minutes are going somewhere, with you
driving.

---

| <- Previous | Next -> |
|---|---|
| [Chapter 22 — Production Operations Runbooks](ch22-data-operations-runbooks.md) | [Chapter 24 — Worked Cases](ch24-worked-cases.md) |
