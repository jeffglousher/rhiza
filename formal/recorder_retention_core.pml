/*
 * Recorder-retention core for SPIN 6.5.2.
 *
 * F is EXCLUSIVE: slots s < F are in the contiguous frontier, are fenced,
 * and may be collected.  This bounded safety model has one fixed context,
 * three recorders, and a two-recorder quorum.  Slot values are collision-free
 * small digest identifiers; frontier digests are their base-4 prefix chain.
 *
 * Acceptance quorum decides an individual slot.  It is deliberately distinct
 * from a frontier certificate: the latter requires two durable recorder
 * attestations with exactly the same (F, digest, context).  No membership
 * transition, ballot/liveness, timing, filesystem corruption, or power loss is
 * modeled here; the successor/transition model owns those concerns.
 */

#define N 3
#define S 3
#define STEPS 5
#define CONFIG 1
#define NONE 0
#define A 1
#define B 2
#define C 3
#define OLD 1
#define FULL 2
#define ABSENT 1
#define PRESENT 2
#define PRUNED_UNAVAILABLE 3
#define CORRUPT_INCOMPLETE 4
#define RESULT_OK 1
#define MUTATION_FENCED 2
#define MUTATION_CONFLICT 3
#define CERT_WRONG_CONTEXT 2
#define CERT_WRONG_DIGEST 3
#define CERT_NONMONOTONIC 4
#define CERT_QUORUM_MISMATCH 5

byte accepted[N*S];          /* recorder*S + slot: durable vote/digest */
byte decided[S];             /* acceptance-quorum decision */
byte certified[S];           /* global exact frontier certificate evidence */
byte cert_frontier, cert_digest, cert_context;
byte attested_frontier[N], attested_digest[N], attested_context[N];
byte installed_frontier[N], installed_digest[N], installed_context[N];
byte live_frontier[N];       /* recovered durable installed view */
byte base_phase[N];          /* 0 old, 1 immutable object, 2 marker/base */
byte suffix_ready[N];        /* committed only after marker/base */
byte authority[N];           /* OLD old-base+WAL, FULL base+suffix */
byte gc_frontier[N], deleted[N*S], tail_ref[S];
byte last_result, last_node, last_slot;
byte mutation_result, proof_result, cert_result;
byte seen_absent, seen_present, seen_pruned, seen_corrupt;
byte rejected_old, rejected_conflict, rejected_subset, rejected_gc;
byte cover_gap, cover_conflict, cover_subset, cover_crash, cover_late, cover_tail;
byte step, i;

#define V(n,s) accepted[n*S+s]
#define QUORUM(s,v) \
 ((V(0,s) == v && V(1,s) == v) || \
  (V(0,s) == v && V(2,s) == v) || \
  (V(1,s) == v && V(2,s) == v))
#define ATTEST_QUORUM(f,d,c) \
 ((attested_frontier[0] == f && attested_digest[0] == d && attested_context[0] == c && \
   attested_frontier[1] == f && attested_digest[1] == d && attested_context[1] == c) || \
  (attested_frontier[0] == f && attested_digest[0] == d && attested_context[0] == c && \
   attested_frontier[2] == f && attested_digest[2] == d && attested_context[2] == c) || \
  (attested_frontier[1] == f && attested_digest[1] == d && attested_context[1] == c && \
   attested_frontier[2] == f && attested_digest[2] == d && attested_context[2] == c))
#define ATTEST_CONTEXT_QUORUM(f,c) \
 ((attested_frontier[0] == f && attested_context[0] == c && \
   attested_frontier[1] == f && attested_context[1] == c) || \
  (attested_frontier[0] == f && attested_context[0] == c && \
   attested_frontier[2] == f && attested_context[2] == c) || \
  (attested_frontier[1] == f && attested_context[1] == c && \
   attested_frontier[2] == f && attested_context[2] == c))

inline digest_matches(f, d) {
  if
  :: f == 0 -> assert(d == NONE)
  :: f == 1 -> assert(d == decided[0])
  :: f == 2 -> assert(d == decided[0]*4+decided[1])
  :: f == 3 -> assert(d == (decided[0]*4+decided[1])*4+decided[2])
  fi
}

inline check_invariants() {
  i = 0;
  do
  :: i < S ->
    assert(certified[i] == NONE || certified[i] == decided[i]);
    assert(certified[i] == NONE || QUORUM(i, certified[i]));
    assert(!(i < cert_frontier && certified[i] == NONE));
    i++
  :: i >= S -> break
  od;
  assert(cert_context == CONFIG);
  digest_matches(cert_frontier, cert_digest);
  i = 0;
  do
  :: i < N ->
    assert(installed_frontier[i] <= cert_frontier);
    assert(installed_frontier[i] <= attested_frontier[i]);
    assert(live_frontier[i] <= installed_frontier[i]);
    assert(gc_frontier[i] <= installed_frontier[i]);
    assert(authority[i] == OLD || authority[i] == FULL);
    assert(authority[i] != FULL || (base_phase[i] == 2 && suffix_ready[i]));
    assert(authority[i] != OLD || !(base_phase[i] == 2 && suffix_ready[i]));
    assert(attested_frontier[i] == 0 || attested_context[i] == CONFIG);
    assert(installed_frontier[i] == 0 || installed_context[i] == CONFIG);
    digest_matches(attested_frontier[i], attested_digest[i]);
    digest_matches(installed_frontier[i], installed_digest[i]);
    i++
  :: i >= N -> break
  od
}

inline accept(n, s, v) {
  if
  :: s < attested_frontier[n] -> rejected_old++; cover_late = 1; mutation_result = MUTATION_FENCED
  :: s >= attested_frontier[n] && V(n,s) == NONE -> V(n,s) = v; mutation_result = RESULT_OK
  :: s >= attested_frontier[n] && V(n,s) == v -> mutation_result = RESULT_OK
  :: s >= attested_frontier[n] && V(n,s) != NONE && V(n,s) != v ->
    rejected_conflict++; cover_conflict = 1; mutation_result = MUTATION_CONFLICT
  fi;
  check_invariants()
}

inline install_proof(n, s, v) {
  if
  :: s < attested_frontier[n] -> cover_late = 1; proof_result = MUTATION_FENCED
  :: s >= attested_frontier[n] && V(n,s) == NONE -> V(n,s) = v; proof_result = RESULT_OK
  :: s >= attested_frontier[n] && V(n,s) == v -> proof_result = RESULT_OK
  :: s >= attested_frontier[n] && V(n,s) != NONE && V(n,s) != v ->
    cover_conflict = 1; proof_result = MUTATION_CONFLICT
  fi;
  check_invariants()
}

inline decide(s, v) {
  if
  :: decided[s] == NONE && QUORUM(s,v) -> decided[s] = v
  :: decided[s] == v -> skip
  :: decided[s] != v && !(decided[s] == NONE && QUORUM(s,v)) -> rejected_conflict++; cover_conflict = 1
  fi;
  check_invariants()
}

inline attest(n, f) {
  if
  :: f == 1 && f > attested_frontier[n] && decided[0] != NONE ->
    attested_frontier[n] = 1; attested_digest[n] = decided[0]; attested_context[n] = CONFIG
  :: f == 2 && f > attested_frontier[n] && decided[0] != NONE && decided[1] != NONE ->
    attested_frontier[n] = 2; attested_digest[n] = decided[0]*4+decided[1]; attested_context[n] = CONFIG
  :: f == 3 && f > attested_frontier[n] && decided[0] != NONE && decided[1] != NONE && decided[2] != NONE ->
    attested_frontier[n] = 3; attested_digest[n] = (decided[0]*4+decided[1])*4+decided[2]; attested_context[n] = CONFIG
  :: !((f == 1 && f > attested_frontier[n] && decided[0] != NONE) ||
       (f == 2 && f > attested_frontier[n] && decided[0] != NONE && decided[1] != NONE) ||
       (f == 3 && f > attested_frontier[n] && decided[0] != NONE && decided[1] != NONE && decided[2] != NONE)) ->
    rejected_conflict++; cover_gap = 1
  fi;
  check_invariants()
}

inline assemble_certificate(f, d, c) {
  if
  :: f > cert_frontier && c == CONFIG && ATTEST_QUORUM(f,d,c) ->
    if
    :: f == 1 -> certified[0] = decided[0]
    :: f == 2 -> certified[0] = decided[0]; certified[1] = decided[1]
    :: f == 3 -> certified[0] = decided[0]; certified[1] = decided[1]; certified[2] = decided[2]
    fi;
    cert_frontier = f; cert_digest = d; cert_context = c; cert_result = RESULT_OK
  :: c != CONFIG -> rejected_conflict++; cover_conflict = 1; cert_result = CERT_WRONG_CONTEXT
  :: c == CONFIG && f <= cert_frontier -> rejected_conflict++; cover_conflict = 1; cert_result = CERT_NONMONOTONIC
  :: c == CONFIG && f > cert_frontier && ATTEST_CONTEXT_QUORUM(f,c) && !ATTEST_QUORUM(f,d,c) ->
    rejected_conflict++; cover_conflict = 1; cert_result = CERT_WRONG_DIGEST
  :: c == CONFIG && f > cert_frontier && !ATTEST_CONTEXT_QUORUM(f,c) ->
    rejected_subset++; cover_subset = 1; cert_result = CERT_QUORUM_MISMATCH
  fi;
  check_invariants()
}

inline install_certificate(n, f, d, c) {
  if
  :: f == cert_frontier && d == cert_digest && c == cert_context && c == CONFIG &&
     f == attested_frontier[n] && f > installed_frontier[n] ->
    installed_frontier[n] = f; installed_digest[n] = d; installed_context[n] = c; live_frontier[n] = f;
    cert_result = RESULT_OK
  :: c != CONFIG || c != cert_context ->
    rejected_conflict++; cover_conflict = 1; cert_result = CERT_WRONG_CONTEXT
  :: c == CONFIG && c == cert_context && d != cert_digest ->
    rejected_conflict++; cover_conflict = 1; cert_result = CERT_WRONG_DIGEST
  :: c == CONFIG && c == cert_context && d == cert_digest && f <= installed_frontier[n] ->
    rejected_conflict++; cover_conflict = 1; cert_result = CERT_NONMONOTONIC
  :: c == CONFIG && c == cert_context && d == cert_digest && f > installed_frontier[n] &&
     !(f == cert_frontier && f == attested_frontier[n]) ->
    rejected_subset++; cover_subset = 1; cert_result = CERT_QUORUM_MISMATCH
  fi;
  check_invariants()
}

inline publish_immutable(n) {
  if
  :: installed_frontier[n] > 0 && base_phase[n] == 0 -> base_phase[n] = 1
  :: !(installed_frontier[n] > 0 && base_phase[n] == 0) -> rejected_conflict++
  fi;
  check_invariants()
}

inline publish_marker(n) {
  if
  :: base_phase[n] == 1 -> base_phase[n] = 2
  :: base_phase[n] != 1 -> rejected_conflict++
  fi;
  check_invariants()
}

inline publish_suffix(n) {
  if
  :: base_phase[n] == 2 -> suffix_ready[n] = 1; authority[n] = FULL
  :: base_phase[n] != 2 -> rejected_conflict++
  fi;
  check_invariants()
}

inline crash_recover(n) {
  cover_crash = 1;
  if
  :: base_phase[n] == 2 && suffix_ready[n] -> authority[n] = FULL
  :: !(base_phase[n] == 2 && suffix_ready[n]) -> authority[n] = OLD
  fi;
  live_frontier[n] = installed_frontier[n];
  check_invariants()
}

inline collect(n, s) {
  if
  :: base_phase[n] == 2 && suffix_ready[n] &&
     s == gc_frontier[n] && s < cert_frontier && s < installed_frontier[n] && !tail_ref[s] ->
    deleted[n*S+s] = 1; gc_frontier[n]++
  :: !(base_phase[n] == 2 && suffix_ready[n] &&
       s == gc_frontier[n] && s < cert_frontier && s < installed_frontier[n] && !tail_ref[s]) ->
    rejected_gc++; cover_tail = 1
  fi;
  check_invariants()
}

inline inspect_old(n, s) {
  last_node = n; last_slot = s;
  if
  :: base_phase[n] == 2 && !suffix_ready[n] -> last_result = CORRUPT_INCOMPLETE; seen_corrupt = 1
  :: !(base_phase[n] == 2 && !suffix_ready[n]) && s < gc_frontier[n] -> last_result = PRUNED_UNAVAILABLE; seen_pruned = 1
  :: !(base_phase[n] == 2 && !suffix_ready[n]) && s >= gc_frontier[n] && s < cert_frontier && certified[s] != NONE -> last_result = PRESENT; seen_present = 1
  :: !(base_phase[n] == 2 && !suffix_ready[n]) && !(s < gc_frontier[n]) && !(s < cert_frontier && certified[s] != NONE) -> last_result = ABSENT; seen_absent = 1
  fi;
  assert(!(last_result == PRESENT && last_slot < gc_frontier[last_node]));
  check_invariants()
}

inline seed_first() {
  accept(0,0,A); accept(1,0,A); decide(0,A);
  attest(0,1); attest(1,1); assemble_certificate(1,A,CONFIG);
  install_certificate(0,1,A,CONFIG)
}

inline schedule_one() {
  if
  :: accept(0,2,A)          /* decided high slot, lower gap still possible */
  :: accept(1,2,A)
  :: decide(2,A)
  :: attest(2,3)            /* rejects until a contiguous decided prefix */
  :: accept(0,1,B)
  :: accept(2,1,B)
  :: decide(1,B)
  :: attest(0,2)
  :: attest(1,2)
  :: assemble_certificate(2,A*4+B,CONFIG)
  :: assemble_certificate(2,B,CONFIG) /* real differing-digest candidate */
  :: assemble_certificate(2,A*4+B,2)   /* real wrong-context candidate */
  :: install_certificate(1,cert_frontier,cert_digest,CONFIG)
  :: install_certificate(0,1,A,CONFIG) /* subset candidate */
  :: accept(0,0,A)
  :: accept(0,0,C)
  :: install_proof(0,0,A)
  :: install_proof(0,2,C)
  :: publish_immutable(0)
  :: publish_marker(0)
  :: publish_suffix(0)
  :: crash_recover(0)
  :: collect(0,0)
  :: collect(0,1)
  :: inspect_old(0,0)
  :: inspect_old(0,2)
  fi
}

inline witness_all() {
  /* high decision with an undecided lower gap cannot be attested */
  accept(0,2,A); accept(1,2,A); decide(2,A); attest(2,3);
  /* form F=2, attest it, crash before assembly, then assemble/install it */
  accept(0,1,B); accept(2,1,B); decide(1,B);
  attest(0,2); attest(1,2); crash_recover(0);
  assemble_certificate(2,A*4+B,CONFIG); install_certificate(0,2,A*4+B,CONFIG); install_certificate(1,2,A*4+B,CONFIG);
  /* F=2 fences only slots below it: slot 1 is closed, slot 2 remains writable. */
  accept(0,1,B); assert(mutation_result == MUTATION_FENCED);
  install_proof(0,1,B); assert(proof_result == MUTATION_FENCED);
  accept(0,2,A); assert(mutation_result == RESULT_OK);
  /* real conflicting candidates, subset install, and old same/conflicting proposals */
  assemble_certificate(2,B,CONFIG); assert(cert_result == CERT_NONMONOTONIC);
  assemble_certificate(2,A*4+B,2); assert(cert_result == CERT_WRONG_CONTEXT);
  install_certificate(0,2,B,CONFIG); assert(cert_result == CERT_WRONG_DIGEST);
  install_certificate(0,2,A*4+B,CONFIG); assert(cert_result == CERT_NONMONOTONIC);
  install_certificate(2,2,A*4+B,CONFIG); assert(cert_result == CERT_QUORUM_MISMATCH);
  accept(0,0,A); assert(mutation_result == MUTATION_FENCED);
  accept(0,2,C); assert(mutation_result == MUTATION_CONFLICT);
  install_proof(0,0,A); assert(proof_result == MUTATION_FENCED);
  install_proof(0,2,C); assert(proof_result == MUTATION_CONFLICT);
  /* immutable, marker, and suffix publication seams all survive recover */
  publish_immutable(0); crash_recover(0); publish_marker(0);
  collect(0,0); assert(gc_frontier[0] == 0 && rejected_gc > 0);
  inspect_old(0,1); crash_recover(0);
  publish_suffix(0); crash_recover(0);
  /* tail reference blocks prefix GC; then deletion exposes pruned and present/absent paths */
  tail_ref[0] = 1; collect(0,0); inspect_old(0,2);
  tail_ref[0] = 0; inspect_old(0,1); collect(0,0); inspect_old(0,0);
  collect(0,1); inspect_old(0,1)
}

init {
  cert_context = CONFIG;
  i = 0;
  do
  :: i < N -> authority[i] = OLD; i++
  :: i >= N -> break
  od;
#ifdef WITNESS_ALL
  seed_first();
  witness_all();
  assert(cover_gap && cover_conflict && cover_subset && cover_crash && cover_late && cover_tail);
  assert(seen_absent && seen_present && seen_pruned && seen_corrupt);
#else
#ifdef SAFETY_POST
  seed_first();
  accept(0,1,B); accept(1,1,B); decide(1,B);
  attest(0,2); attest(1,2); assemble_certificate(2,A*4+B,CONFIG); install_certificate(0,2,A*4+B,CONFIG);
#else
#ifdef SAFETY_SEEDED
  seed_first();
#endif
#endif
  step = 0;
  do
  :: step < STEPS -> schedule_one(); step++
  :: step >= STEPS -> break
  od;
#endif
  check_invariants()
}
