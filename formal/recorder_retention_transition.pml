/*
 * Recorder retention: bounded STOP(S)->Activate(S+1) control-plane model.
 * SPIN 6.5.2, five nodes: old={0,1,2}, successor={2,3,4}; each Q=2.
 *
 * A1: per-context agreement.  A2: only an exact, quorum-backed proof changes
 * transition state.  A3: collection waits for every retained transition ref,
 * including a NodeValidatedCheckpointReceipt until completed release.
 * This is deliberately not the three-recorder data-plane core model.
 * It does not prove liveness, ballots, arbitrary memberships, durable node
 * recovery, or NodeValidatedCheckpointReceipt availability.  A receipt, when
 * present, is checked here only for its exact context and anchor.
 */

#define N 5
#define OLD_N 3
#define STEPS 9
#define STOP 7
#define NEXT 8
#define ANCHOR 5
#define NONE 0
#define INTENT 1
#define CONFIG_HEAD 2
#define COMMITTED 3
#define OLD_OPEN 0
#define OLD_SEALED 1
#define SUCC_PENDING 2
#define SUCC_ACTIVE 3
#define REJECTED 1
#define ACCEPTED 2

byte node_state[N];
byte stop_sig[OLD_N], next_sig[OLD_N], nonstop_proof[OLD_N];
/* next_sig index: node 2,3,4; nonstop_proof is distinct from STOP signing. */
byte stop_cert, activation_cert, publish, barrier, checkpoint, refs_released;
byte stop_ref, activation_ref, checkpoint_ref, old_ref[OLD_N], retained[OLD_N];
byte receipt[N], receipt_context[N], receipt_anchor[N];
byte old_write_result, proof_result, cert_result, gc_result;
byte later_old_value, rejected_old_write, rejected_candidate, rejected_gc;
byte snapshot_old_value, snapshot_stop_cert, snapshot_publish, snapshot_node0;
byte cover_pre_next, cover_late_signer, cover_post_write, cover_provisional;
byte step;

#define OLD_Q2_STOP \
 ((stop_sig[0] == STOP && stop_sig[1] == STOP) || \
  (stop_sig[0] == STOP && stop_sig[2] == STOP) || \
  (stop_sig[1] == STOP && stop_sig[2] == STOP))
#define SUCC_Q2_NEXT \
 ((next_sig[0] == NEXT && next_sig[1] == NEXT) || \
  (next_sig[0] == NEXT && next_sig[2] == NEXT) || \
  (next_sig[1] == NEXT && next_sig[2] == NEXT))
#define RELEASE_Q2 \
 ((nonstop_proof[0] == STOP && nonstop_proof[1] == STOP) || \
  (nonstop_proof[0] == STOP && nonstop_proof[2] == STOP) || \
  (nonstop_proof[1] == STOP && nonstop_proof[2] == STOP))
#define ALL_SUCC_ACTIVE (node_state[2] == SUCC_ACTIVE && node_state[3] == SUCC_ACTIVE && node_state[4] == SUCC_ACTIVE)
#define ANY_REF (stop_ref || activation_ref || checkpoint_ref || old_ref[0] || old_ref[1] || old_ref[2] || receipt[0] || receipt[1] || receipt[2] || receipt[3] || receipt[4])

inline check_invariants() {
  assert(!stop_cert || OLD_Q2_STOP);
  assert(!activation_cert || SUCC_Q2_NEXT);
  assert(!activation_cert || stop_cert);
  assert(publish < CONFIG_HEAD || activation_cert);
  assert(publish != COMMITTED || (barrier && checkpoint));
  assert(node_state[3] != SUCC_ACTIVE || publish == COMMITTED);
  assert(node_state[4] != SUCC_ACTIVE || publish == COMMITTED);
  assert(node_state[2] != SUCC_ACTIVE || publish == COMMITTED);
  assert(!refs_released || !ANY_REF);
  assert(stop_cert || node_state[0] != OLD_SEALED || RELEASE_Q2);
  assert(stop_cert || node_state[1] != OLD_SEALED || RELEASE_Q2);
  assert(stop_cert || node_state[2] != OLD_SEALED || RELEASE_Q2);
  assert(!(retained[0] == 0 || retained[1] == 0 || retained[2] == 0) || !ANY_REF);
  assert(!receipt[0] || (receipt_context[0] == NEXT && receipt_anchor[0] == ANCHOR));
  assert(!receipt[1] || (receipt_context[1] == NEXT && receipt_anchor[1] == ANCHOR));
  assert(!receipt[2] || (receipt_context[2] == NEXT && receipt_anchor[2] == ANCHOR));
  assert(!receipt[3] || (receipt_context[3] == NEXT && receipt_anchor[3] == ANCHOR));
  assert(!receipt[4] || (receipt_context[4] == NEXT && receipt_anchor[4] == ANCHOR));
}

inline stop_sign(n, value, anchor) {
  if
  :: !stop_cert && n < OLD_N && value == STOP && anchor == ANCHOR && stop_sig[n] == NONE ->
    stop_sig[n] = STOP; proof_result = ACCEPTED
  :: !(!stop_cert && n < OLD_N && value == STOP && anchor == ANCHOR && stop_sig[n] == NONE) -> rejected_candidate++; proof_result = REJECTED
  fi;
  check_invariants()
}

inline next_sign(n, value, anchor) {
  if
  :: n >= 2 && n < N && value == NEXT && anchor == ANCHOR && next_sig[n-2] == NONE ->
    next_sig[n-2] = NEXT; proof_result = ACCEPTED
  :: !(n >= 2 && n < N && value == NEXT && anchor == ANCHOR && next_sig[n-2] == NONE) -> rejected_candidate++; proof_result = REJECTED
  fi;
  check_invariants()
}

inline certify_stop(value, anchor) {
  if
  :: !stop_cert && value == STOP && anchor == ANCHOR && OLD_Q2_STOP ->
    stop_cert = 1; stop_ref = 1; old_ref[0] = 1; old_ref[1] = 1; old_ref[2] = 1;
    node_state[0] = OLD_SEALED; node_state[1] = OLD_SEALED; node_state[2] = OLD_SEALED; cert_result = ACCEPTED
  :: !(!stop_cert && value == STOP && anchor == ANCHOR && OLD_Q2_STOP) -> rejected_candidate++; cert_result = REJECTED
  fi;
  check_invariants()
}

inline certify_activation(value, anchor) {
  if
  :: stop_cert && !activation_cert && value == NEXT && anchor == ANCHOR && SUCC_Q2_NEXT ->
    activation_cert = 1; activation_ref = 1; cert_result = ACCEPTED
  :: !(stop_cert && !activation_cert && value == NEXT && anchor == ANCHOR && SUCC_Q2_NEXT) -> rejected_candidate++; cert_result = REJECTED
  fi;
  check_invariants()
}

inline certify_nonstop_proof(n, value, anchor) {
  if
  :: !stop_cert && n < OLD_N && value == STOP && anchor == ANCHOR && nonstop_proof[n] == NONE ->
    nonstop_proof[n] = STOP; proof_result = ACCEPTED
  :: !(!stop_cert && n < OLD_N && value == STOP && anchor == ANCHOR && nonstop_proof[n] == NONE) -> rejected_candidate++; proof_result = REJECTED
  fi;
  check_invariants()
}

inline provisional_release(n, value, anchor) {
  if
  :: !stop_cert && n < OLD_N && value == STOP && anchor == ANCHOR && RELEASE_Q2 ->
    node_state[n] = OLD_SEALED; proof_result = ACCEPTED; cover_provisional = 1
  :: !(!stop_cert && n < OLD_N && value == STOP && anchor == ANCHOR && RELEASE_Q2) -> rejected_candidate++; proof_result = REJECTED
  fi;
  check_invariants()
}

inline old_write(n) {
  if
  :: n < OLD_N && stop_cert -> rejected_old_write++; old_write_result = REJECTED
  :: n < OLD_N && !stop_cert -> later_old_value = 1; old_write_result = ACCEPTED
  :: !(n < OLD_N) -> rejected_candidate++; old_write_result = REJECTED
  fi;
  check_invariants()
}

inline publish_intent() {
  if
  :: stop_cert && publish == NONE -> publish = INTENT
  :: !(stop_cert && publish == NONE) -> rejected_candidate++
  fi;
  check_invariants()
}

inline publish_config_head(anchor) {
  if
  :: activation_cert && publish == INTENT && anchor == ANCHOR -> publish = CONFIG_HEAD
  :: !(activation_cert && publish == INTENT && anchor == ANCHOR) -> rejected_candidate++
  fi;
  check_invariants()
}

inline reach_barrier() {
  if
  :: publish == CONFIG_HEAD && activation_cert -> barrier = 1
  :: !(publish == CONFIG_HEAD && activation_cert) -> rejected_candidate++
  fi;
  check_invariants()
}

inline install_checkpoint() {
  if
  :: barrier && publish == CONFIG_HEAD -> checkpoint = 1; checkpoint_ref = 1
  :: !(barrier && publish == CONFIG_HEAD) -> rejected_candidate++
  fi;
  check_invariants()
}

inline validated_checkpoint_receipt(n, context, anchor) {
  if
  :: !refs_released && n < N && checkpoint && context == NEXT && anchor == ANCHOR ->
    receipt[n] = 1; receipt_context[n] = context; receipt_anchor[n] = anchor
  :: !(!refs_released && n < N && checkpoint && context == NEXT && anchor == ANCHOR) -> rejected_candidate++
  fi;
  check_invariants()
}

inline publish_commit() {
  if
  :: publish == CONFIG_HEAD && barrier && checkpoint -> publish = COMMITTED
  :: !(publish == CONFIG_HEAD && barrier && checkpoint) -> rejected_candidate++
  fi;
  check_invariants()
}

inline activate_successor(n) {
  if
  :: publish == COMMITTED && n >= 2 && n < N -> node_state[n] = SUCC_ACTIVE
  :: !(publish == COMMITTED && n >= 2 && n < N) -> rejected_candidate++
  fi;
  check_invariants()
}

inline crash_recover() {
  if
  :: publish != COMMITTED -> publish = NONE; barrier = 0; checkpoint = 0; checkpoint_ref = 0
  :: publish == COMMITTED -> skip
  fi;
  check_invariants()
}

inline release_transition_refs() {
  if
  :: publish == COMMITTED && ALL_SUCC_ACTIVE ->
    stop_ref = 0; activation_ref = 0; checkpoint_ref = 0; old_ref[0] = 0; old_ref[1] = 0; old_ref[2] = 0;
    receipt[0] = 0; receipt[1] = 0; receipt[2] = 0; receipt[3] = 0; receipt[4] = 0; refs_released = 1
  :: !(publish == COMMITTED && ALL_SUCC_ACTIVE) -> rejected_candidate++
  fi;
  check_invariants()
}

inline collect_old(n) {
  if
  :: n < OLD_N && retained[n] && stop_cert && activation_cert && publish == COMMITTED && ALL_SUCC_ACTIVE && !ANY_REF && (node_state[n] == OLD_SEALED || (n == 2 && node_state[n] == SUCC_ACTIVE)) -> retained[n] = 0; gc_result = ACCEPTED
  :: !(n < OLD_N && retained[n] && stop_cert && activation_cert && publish == COMMITTED && ALL_SUCC_ACTIVE && !ANY_REF && (node_state[n] == OLD_SEALED || (n == 2 && node_state[n] == SUCC_ACTIVE))) -> rejected_gc++; gc_result = REJECTED
  fi;
  check_invariants()
}

inline seed_stop() {
  stop_sign(0,STOP,ANCHOR); stop_sign(1,STOP,ANCHOR); certify_stop(STOP,ANCHOR)
}

inline seed_install() {
  seed_stop(); next_sign(3,NEXT,ANCHOR); next_sign(4,NEXT,ANCHOR);
  certify_activation(NEXT,ANCHOR); publish_intent(); publish_config_head(ANCHOR);
  reach_barrier(); install_checkpoint(); publish_commit()
}

inline witness() {
  /* Disjoint stop quorum {0,1}; successor quorum {3,4}. */
  next_sign(3,NEXT,ANCHOR); assert(proof_result == ACCEPTED); cover_pre_next = 1;
  old_write(0); assert(old_write_result == ACCEPTED && later_old_value);
  stop_sign(0,STOP,ANCHOR); provisional_release(0,STOP,ANCHOR); assert(proof_result == REJECTED);
  certify_nonstop_proof(0,STOP,ANCHOR); certify_nonstop_proof(1,STOP,ANCHOR);
  provisional_release(0,STOP,ANCHOR); assert(proof_result == ACCEPTED && node_state[0] == OLD_SEALED);
  seed_stop();
  stop_sign(2,STOP,ANCHOR); assert(proof_result == REJECTED); cover_late_signer = 1;
  snapshot_old_value = later_old_value; snapshot_stop_cert = stop_cert; snapshot_publish = publish; snapshot_node0 = node_state[0];
  old_write(0); assert(old_write_result == REJECTED); assert(later_old_value == snapshot_old_value && stop_cert == snapshot_stop_cert && publish == snapshot_publish && node_state[0] == snapshot_node0); cover_post_write = 1;
  provisional_release(2,STOP,ANCHOR); assert(proof_result == REJECTED);
  certify_stop(STOP+1,ANCHOR); assert(cert_result == REJECTED);
  next_sign(4,NEXT,ANCHOR); certify_activation(NEXT,ANCHOR);
  publish_intent(); publish_config_head(ANCHOR); reach_barrier(); install_checkpoint();
  validated_checkpoint_receipt(3,NEXT,ANCHOR); publish_commit();
  activate_successor(2); activate_successor(3); activate_successor(4); collect_old(0); assert(gc_result == REJECTED); release_transition_refs();
  assert(cover_pre_next && cover_late_signer && cover_post_write && cover_provisional);
  collect_old(0); collect_old(1); collect_old(2); assert(retained[0] == 0 && retained[1] == 0 && retained[2] == 0)
}

proctype Scheduler() {
  do
  :: step < STEPS ->
    if
    :: stop_sign(0,STOP,ANCHOR)
    :: stop_sign(1,STOP,ANCHOR)
    :: stop_sign(2,STOP,ANCHOR)
    :: stop_sign(0,STOP+1,ANCHOR)
    :: stop_sign(0,STOP,ANCHOR+1)
    :: certify_nonstop_proof(0,STOP,ANCHOR)
    :: certify_nonstop_proof(1,STOP,ANCHOR)
    :: certify_nonstop_proof(2,STOP,ANCHOR)
    :: provisional_release(0,STOP,ANCHOR)
    :: certify_stop(STOP,ANCHOR)
    :: certify_stop(STOP+1,ANCHOR)
    :: old_write(0)
    :: next_sign(2,NEXT,ANCHOR)
    :: next_sign(3,NEXT,ANCHOR)
    :: next_sign(4,NEXT,ANCHOR)
    :: next_sign(3,NEXT+1,ANCHOR)
    :: certify_activation(NEXT,ANCHOR)
    :: certify_activation(NEXT+1,ANCHOR)
    :: publish_intent()
    :: publish_config_head(ANCHOR)
    :: publish_config_head(ANCHOR+1)
    :: reach_barrier()
    :: install_checkpoint()
    :: validated_checkpoint_receipt(3,NEXT,ANCHOR)
    :: validated_checkpoint_receipt(3,STOP,ANCHOR)
    :: publish_commit()
    :: activate_successor(2)
    :: activate_successor(3)
    :: activate_successor(4)
    :: release_transition_refs()
    :: collect_old(0)
    :: crash_recover()
    fi;
    step++
  :: else -> break
  od;
  check_invariants()
}

init {
  atomic {
    node_state[0] = OLD_OPEN; node_state[1] = OLD_OPEN; node_state[2] = OLD_OPEN;
    node_state[3] = SUCC_PENDING; node_state[4] = SUCC_PENDING;
    retained[0] = 1; retained[1] = 1; retained[2] = 1;
#ifdef WITNESS_ALL
    witness()
#elif defined(PROFILE_POST_ACTIVATION)
    seed_install(); activate_successor(2); activate_successor(3); activate_successor(4)
#elif defined(PROFILE_POST_INSTALL)
    seed_install()
#elif defined(PROFILE_POST_STOP)
    seed_stop()
#endif
    check_invariants()
#ifndef WITNESS_ALL
    run Scheduler()
#endif
  }
}
