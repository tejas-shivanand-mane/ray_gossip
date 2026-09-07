// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/recovery_frontier.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray::core {

/// Patch 4D: pipelined holder admission.
/// Patch 4F: first-holder TaskSpec piggyback.
/// Patch 4G: hot-path profiling.
/// Patch 4H: compact task-argument recovery metadata.
/// Patch 4I: TaskSpec-level recovery argument sidecar.
/// Patch 4J: task-centric recovery state and on-demand owner lineage.
/// Patch 4L: correctness-preserving retained owner lineage for late borrow.
/// Stores lineage, succession, and holder-admission state for the
/// experimental recovery-succession implementation.
class RecoverySuccessionManager {
 public:
  struct CandidateReport {
    rpc::Address coordinator_address;
    rpc::ReportRecoveryCandidateRequest request;
  };

  struct HolderAdmissionPlan {
    std::string reservation_id;
    rpc::Address candidate_address;
    rpc::TaskSpec task_spec;
    rpc::RecoveryManifest proposed_manifest;

    // True when the candidate already has the complete producer TaskSpec.
    // Patch 4F uses this for a downstream borrower that consumed the
    // transport-only TaskSpec sidecar. This does not imply commitment.
    bool candidate_already_stores_task_spec = false;
  };

  struct RecoverySuccessionProfile {
    uint64_t holder_recipe_copies_avoided = 0;
    uint64_t holder_recipe_fallback_copies = 0;
    uint64_t shared_holder_recipes_current = 0;
    // Serialized recipe bytes referenced by shared states, not heap/RSS bytes.
    uint64_t shared_holder_recipe_bytes_current = 0;
    uint64_t candidate_reports_received = 0;
    uint64_t candidate_reports_accepted = 0;

    uint64_t holder_install_rpcs_sent = 0;
    uint64_t holder_install_rpcs_completed = 0;

    uint64_t holder_commit_rpcs_sent = 0;
    uint64_t holder_commit_rpcs_completed = 0;

    uint64_t witness_update_rpcs_sent = 0;
    uint64_t witness_update_rpcs_completed = 0;

    // Benchmark-70 witness barrier decomposition. All timing sums are per
    // logical witness update; physical batch counters are derived from the
    // client-side demultiplexing metadata.
    uint64_t witness_update_client_queue_time_ns = 0;
    uint64_t witness_update_client_submit_to_cq_time_ns = 0;
    uint64_t witness_update_client_cq_to_main_loop_time_ns = 0;
    uint64_t witness_update_client_main_loop_to_batch_callback_time_ns = 0;
    uint64_t witness_update_client_phase_samples = 0;
    uint64_t witness_update_server_batch_queue_time_ns = 0;
    uint64_t witness_update_handler_time_ns = 0;
    uint64_t witness_update_handler_samples = 0;
    uint64_t witness_update_mutex_wait_time_ns = 0;
    uint64_t witness_update_mutex_hold_time_ns = 0;
    uint64_t witness_update_physical_batches_completed = 0;
    uint64_t witness_update_physical_batch_items = 0;

    // Benchmark-71 profiling-only synchronous CPU service attribution.
    uint64_t witness_update_client_enqueue_cpu_time_ns = 0;
    uint64_t witness_update_client_batch_build_cpu_time_ns = 0;
    uint64_t witness_update_client_batch_demux_cpu_time_ns = 0;

    uint64_t holder_admission_prepare_cpu_calls = 0;
    uint64_t holder_admission_prepare_cpu_time_ns = 0;
    uint64_t witness_request_build_cpu_calls = 0;
    uint64_t witness_request_build_cpu_time_ns = 0;
    uint64_t witness_logical_callback_cpu_calls = 0;
    uint64_t witness_logical_callback_cpu_time_ns = 0;
    uint64_t witness_winner_callback_cpu_calls = 0;
    uint64_t witness_winner_callback_cpu_time_ns = 0;
    uint64_t witness_redundant_callback_cpu_calls = 0;
    uint64_t witness_redundant_callback_cpu_time_ns = 0;
    uint64_t holder_commit_cpu_calls = 0;
    uint64_t holder_commit_cpu_time_ns = 0;

    // Benchmark-74 initial installation service elapsed time (steady clock).
    // Includes lock waits/preemption. Materialization is nested inside the
    // install handler; encoding/callback rows also overlap admission timings.
    uint64_t frontier_recipe_encode_calls = 0;
    uint64_t frontier_recipe_encode_time_ns = 0;
    uint64_t frontier_recipe_encode_members = 0;
    uint64_t frontier_recipe_encode_bytes = 0;
    uint64_t holder_install_handler_calls = 0;
    uint64_t holder_install_handler_time_ns = 0;
    uint64_t frontier_holder_materialize_calls = 0;
    uint64_t frontier_holder_materialize_time_ns = 0;
    uint64_t frontier_holder_materialize_members = 0;
    uint64_t holder_install_callback_calls = 0;
    uint64_t holder_install_callback_time_ns = 0;
    uint64_t frontier_recipe_piggybacks_sent = 0;
    uint64_t frontier_recipe_piggyback_bytes_sent = 0;
    uint64_t frontier_recipe_piggybacks_stored = 0;
    uint64_t frontier_recipe_piggyback_store_time_ns = 0;
    uint64_t frontier_recipe_piggyback_admissions = 0;

    // Opportunistic H2 readiness sampled at the instant ordinary K=1 H1
    // begins witness publication. This does not delay H1.
    uint64_t h1_publish_readiness_samples = 0;
    uint64_t h2_reserved_at_h1_publish = 0;
    uint64_t h2_installed_at_h1_publish = 0;
    uint64_t h1_ack_readiness_samples = 0;
    uint64_t h2_reserved_at_h1_ack = 0;
    uint64_t h2_installed_at_h1_ack = 0;

    // Wall-clock latency of the whole witness-publication stage.
    // This is different from witness_update_rpc_time_ns, which sums
    // the RTTs of individual witness RPCs.
    uint64_t witness_publish_count = 0;
    uint64_t witness_publish_time_ns = 0;
    uint64_t witness_publish_max_time_ns = 0;

    uint64_t task_spec_bytes_sent = 0;
    uint64_t manifest_bytes_sent = 0;

    uint64_t owner_task_spec_copy_count = 0;
    uint64_t owner_task_spec_copy_time_ns = 0;

    // Legacy Patch-4J metric. Patch 4L deliberately retains one dormant
    // owner TaskSpec, so this remains zero under the 4L design.
    uint64_t owner_lazy_task_spec_copies_avoided = 0;

    // Patch 4L owner-retained lineage accounting. "current" and "peak" are
    // gauges/state high-water marks; created/released are cumulative events
    // since the last profile reset.
    uint64_t owner_retained_task_specs_current = 0;
    uint64_t owner_retained_task_specs_peak = 0;
    uint64_t owner_retained_task_spec_bytes_current = 0;
    uint64_t owner_retained_task_spec_bytes_peak = 0;
    uint64_t owner_retained_task_specs_created = 0;
    uint64_t owner_retained_task_specs_released = 0;
    uint64_t owner_retained_task_spec_copy_time_ns = 0;

    uint64_t task_centric_metadata_builds = 0;

    // Patch 4F full-lineage copies moved through normal downstream PushTask
    // transport instead of InstallRecoveryHolder. task_spec_bytes_sent also
    // includes these bytes.
    uint64_t first_holder_piggyback_copies_sent = 0;
    uint64_t first_holder_piggyback_bytes_sent = 0;
    uint64_t first_holder_piggyback_serialize_time_ns = 0;

    uint64_t holder_install_rpc_time_ns = 0;
    uint64_t holder_commit_rpc_time_ns = 0;
    uint64_t witness_update_rpc_time_ns = 0;

    uint64_t holder_admissions_committed = 0;
    uint64_t holder_admission_time_ns = 0;
    uint64_t holder_admission_max_time_ns = 0;

    uint64_t manifest_generations_committed = 0;
    uint64_t max_generation = 0;
    uint64_t max_non_owner_holders = 0;
    uint64_t frozen_commits = 0;

    // Recovery work performed before any downstream holder admission.
    uint64_t task_argument_metadata_calls = 0;
    uint64_t task_argument_metadata_time_ns = 0;

    // Patch 4H: base recovery metadata bytes attached to normal TaskSpec
    // arguments, excluding the optional first-holder TaskSpec sidecar.
    uint64_t task_argument_metadata_refs_attached = 0;
    uint64_t task_argument_metadata_compact_refs = 0;
    uint64_t task_argument_metadata_compact_fallbacks = 0;
    uint64_t task_argument_metadata_full_bytes_equivalent = 0;
    uint64_t task_argument_metadata_transport_bytes = 0;

    uint64_t initial_manifest_build_count = 0;
    uint64_t initial_manifest_build_time_ns = 0;
    uint64_t initial_manifest_bytes = 0;

    uint64_t witness_selection_count = 0;
    uint64_t witness_selection_time_ns = 0;

    uint64_t witness_gcs_query_count = 0;
    uint64_t witness_gcs_query_time_ns = 0;

    uint64_t task_spec_manifest_attach_count = 0;
    uint64_t task_spec_manifest_attach_time_ns = 0;

    uint64_t register_owned_task_count = 0;
    uint64_t register_owned_task_time_ns = 0;

    // Patch 4G: synchronous hot-path costs. These are CPU/wall-clock durations
    // spent inside the calling thread, not asynchronous control-RPC latency.
    uint64_t recovery_metadata_lookup_calls = 0;
    uint64_t recovery_metadata_lookup_hits = 0;
    uint64_t recovery_metadata_lookup_time_ns = 0;

    uint64_t ensure_task_arguments_calls = 0;
    uint64_t ensure_task_arguments_time_ns = 0;

    uint64_t register_executor_task_calls = 0;
    uint64_t register_executor_task_time_ns = 0;
    uint64_t register_executor_metadata_refs_seen = 0;
    uint64_t register_executor_candidate_reports_built = 0;

    uint64_t candidate_report_build_calls = 0;
    uint64_t candidate_reports_built = 0;
    uint64_t candidate_report_build_time_ns = 0;

    uint64_t candidate_queue_calls = 0;
    uint64_t candidate_queue_time_ns = 0;

    // Candidate-report transport. logical_reports counts individual tasks;
    // physical_rpcs counts actual single/batched gRPCs.
    uint64_t candidate_rpc_logical_reports_sent = 0;
    uint64_t candidate_rpc_logical_reports_completed = 0;
    uint64_t candidate_rpc_physical_rpcs_sent = 0;
    uint64_t candidate_rpc_physical_rpcs_completed = 0;
    uint64_t candidate_rpc_request_bytes_sent = 0;
    uint64_t candidate_rpc_time_ns = 0;
  };

  RecoverySuccessionProfile GetProfileSnapshot() const;

  void ResetProfile();

  void RecordCandidateReport(bool accepted);

  void RecordHolderInstallRpcSent(uint64_t task_spec_bytes,
                                  uint64_t manifest_bytes);

  void RecordHolderInstallRpcLatency(uint64_t latency_ns);

  void RecordFrontierRecipeEncoding(uint64_t elapsed_ns,
                                    uint64_t members,
                                    uint64_t bytes);
  void RecordHolderInstallHandler(uint64_t elapsed_ns);
  void RecordHolderInstallCallback(uint64_t elapsed_ns);

  void RecordOwnerTaskSpecCopyLatency(uint64_t latency_ns);

  void RecordWitnessUpdateRpcSent(uint64_t task_spec_bytes,
                                  uint64_t manifest_bytes);

  void RecordWitnessUpdateRpcLatency(uint64_t latency_ns);

  void RecordWitnessUpdateRpcBreakdown(
      uint64_t client_queue_ns,
      uint64_t client_submit_to_cq_ns,
      uint64_t client_cq_to_main_loop_ns,
      uint64_t client_main_loop_to_batch_callback_ns,
      uint64_t client_enqueue_cpu_ns,
      uint64_t client_batch_build_cpu_ns,
      uint64_t client_batch_demux_cpu_ns,
      uint64_t server_batch_queue_ns,
      uint64_t handler_ns,
      uint64_t mutex_wait_ns,
      uint64_t mutex_hold_ns,
      bool batch_leader,
      uint32_t batch_size);

  void RecordHolderAdmissionPrepareCpu(uint64_t latency_ns);
  void RecordWitnessRequestBuildCpu(uint64_t latency_ns);
  void RecordWitnessLogicalCallbackCpu(uint64_t latency_ns, bool winner);
  void RecordHolderCommitCpu(uint64_t latency_ns);

  void RecordH2ReadinessAtH1Publish(bool h2_reserved, bool h2_installed);
  void RecordH2ReadinessAtH1Ack(bool h2_reserved, bool h2_installed);

  void RecordWitnessPublishLatency(uint64_t latency_ns);

  void RecordHolderCommitRpcSent(uint64_t manifest_bytes);

  void RecordHolderCommitRpcLatency(uint64_t latency_ns);

  void RecordHolderAdmissionLatency(uint64_t latency_ns);

  void RecordTaskArgumentMetadataLatency(uint64_t latency_ns);

  void RecordInitialManifestBuild(uint64_t latency_ns,
                                  uint64_t manifest_bytes);

  void RecordWitnessSelectionLatency(uint64_t latency_ns);

  void RecordWitnessGcsQueryLatency(uint64_t latency_ns);

  void RecordTaskSpecManifestAttachLatency(uint64_t latency_ns);

  void RecordRegisterOwnedTaskLatency(uint64_t latency_ns);

  // Patch 4G hot-path profiling.
  void RecordEnsureTaskArgumentsLatency(uint64_t latency_ns);
  void RecordCandidateQueueLatency(uint64_t latency_ns);
  void RecordCandidateRpcSent(uint64_t logical_reports, uint64_t request_bytes);
  void RecordCandidateRpcLatency(uint64_t logical_reports, uint64_t latency_ns);

  explicit RecoverySuccessionManager(rpc::Address self_address);

  RecoverySuccessionManager(const RecoverySuccessionManager &) = delete;
  RecoverySuccessionManager &operator=(const RecoverySuccessionManager &) = delete;

  using AdaptiveFrontierPublisher = std::function<bool(
      const rpc::RecoveryManifest &, const rpc::RecoveryFrontierAppend &)>;

  /// Registers the process-local CoreWorker transport used to make adaptive
  /// Frontier recipe suffixes durable on the already-admitted H1..HR holders.
  /// Standalone manager tests may leave this unset.
  static void RegisterAdaptiveFrontierPublisher(AdaptiveFrontierPublisher publisher);

  /// Returns the one Recovery Succession manager owned by this CoreWorker
  /// process. CoreWorker's RPC proxy uses this only for typed Frontier-append
  /// requests; normal recovery RPCs continue through CoreWorker itself.
  static RecoverySuccessionManager *GetProcessRecoveryManager() {
    return process_recovery_manager_.load(std::memory_order_acquire);
  }

  /// Returns true when recovery succession supports the task.
  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);

  /// Returns whether owner-side correctness-capable Recovery Frontier
  /// grouping is enabled for this manager. Configuration is immutable after
  /// construction, so this check is lock-free.
  bool RecoveryFrontierEnabled() const;

  /// Assign an eligible owner task to its append-only frontier group. The
  /// first member becomes the immediately protectable group leader.
  std::optional<RecoveryFrontierMembership> RegisterOwnerTaskWithRecoveryFrontier(
      const TaskSpecification &task_spec);

  /// Return stable group coordinates for a previously registered owner task.
  std::optional<RecoveryFrontierMembership> GetRecoveryFrontierMembership(
      const TaskID &task_id) const;

  /// Return the immutable protection manifest selected for a frontier group.
  /// The group leader TaskID is the manifest TaskID.
  bool GetRecoveryFrontierProtectionManifest(
      const TaskID &group_id, rpc::RecoveryManifest *manifest) const;

  /// Cache the first protection manifest selected for a group and return the
  /// authoritative cached value. Concurrent first activations therefore cannot
  /// split later members across different fixed-R holder sets.
  bool CacheRecoveryFrontierProtectionManifest(
      const rpc::RecoveryManifest &candidate,
      rpc::RecoveryManifest *authoritative_manifest);

  /// Stage/commit/abort the next contiguous group append. These methods expose
  /// the frontier acknowledged-prefix state machine to either protection
  /// backend without coupling the planner to Baseline or Succession RPCs.
  std::optional<RecoveryFrontierAppendBatch> StageRecoveryFrontierAppend(
      const TaskID &group_id, uint32_t max_batch_members = 0);

  /// True while at least one member of the group remains outside the
  /// acknowledged durable prefix. Used by synchronous object export to
  /// wait behind an append already being published by another thread.
  bool RecoveryFrontierGroupHasUncommittedMembers(const TaskID &group_id) const;

  bool CommitRecoveryFrontierAppend(const RecoveryFrontierAppendBatch &batch);
  bool AbortRecoveryFrontierAppend(const RecoveryFrontierAppendBatch &batch);

  /// Serialize one staged append using the shared holder wire format.
  static bool BuildRecoveryFrontierAppendProto(
      const RecoveryFrontierAppendBatch &batch,
      rpc::RecoveryFrontierAppend *append);

  /// Commit an adaptive Frontier recipe suffix on the owner after every
  /// already-admitted Succession holder ACKed that exact append.
  bool CommitAdaptiveRecoveryFrontierAppend(
      const RecoveryFrontierAppendBatch &batch,
      const rpc::RecoveryManifest &group_manifest);

  /// Holder-side import of a committed adaptive Frontier recipe suffix.
  /// The shared Succession topology is unchanged; only the newly appended
  /// member TaskSpecs become replayable.
  bool ApplyAdaptiveRecoveryFrontierAppend(
      const rpc::RecoveryFrontierAppend &append,
      const rpc::RecoveryManifest &group_manifest);

  /// Resolve a committed group-global return index back to the original task
  /// replay recipe. Uncommitted members deliberately return false.
  bool ExtractRecoveryFrontierTaskForReturn(const TaskID &group_id,
                                            uint32_t group_return_index,
                                            rpc::TaskSpec *task_spec,
                                            uint32_t *task_return_index) const;

  /// Returns true only when a task actually carries Recovery Succession
  /// state: either its own recovery manifest or recovery metadata on one of
  /// its ObjectRef arguments.
  static bool CarriesRecoveryMetadata(const rpc::TaskSpec &task_spec);

  /// Creates the initial manifest owned by this CoreWorker.
  rpc::RecoveryManifest BuildInitialManifest(const TaskID &task_id,
                                             const JobID &job_id,
                                             int32_t max_retries) const;

  /// Records a task whose TaskSpec already carries a recovery manifest and
  /// attaches metadata to its return refs. This path is also used by recovery replay.
  void RegisterOwnedTask(const TaskSpecification &task_spec,
                         std::vector<rpc::ObjectReference> *returned_refs);

  /// Lazily installs owner recovery state after a task return is actually
  /// exported/borrowed. The TaskSpec does not need to already carry a
  /// recovery_manifest; this stores a private replayable copy with the
  /// supplied manifest attached and creates metadata for all static returns.
  /// Returns true only if this call performed the initialization.
  bool RegisterOwnedTaskLazy(const TaskSpecification &task_spec,
                             const rpc::RecoveryManifest &manifest);

  /// Retain owner-side lifetime state while at least one static return ObjectRef
  /// is truly in scope. Production CoreWorker callers set
  /// task_manager_owns_recipe=true and use TaskManager as the sole dormant
  /// TaskSpec owner; the manager-owned TaskSpec remains only as a compatibility
  /// fallback for direct manager tests/non-CoreWorker callers.
  void RetainOwnerTaskSpecForLazyRecovery(
      const TaskSpecification &task_spec,
      const std::vector<rpc::ObjectReference> &returned_refs,
      bool task_manager_owns_recipe = false);

  /// Copies the retained owner TaskSpec if one is still live.
  bool GetRetainedOwnerTaskSpec(const TaskID &task_id,
                                rpc::TaskSpec *task_spec) const;

  /// True while a legacy/Fixed-R owner-retained task still has at least
  /// one live returned ObjectRef.
  bool OwnerTaskHasLiveReturns(const TaskID &task_id) const;

  /// Adaptive-Succession owner cleanup driven by TaskManager's existing
  /// reconstructable-return lifetime. Returns true iff remote recovery state
  /// for this task/frontier group should now be tombstoned.
  bool HandleOwnerTaskLineageReleased(const TaskID &task_id);

  /// Records actual ObjectRef deletion. Returns true iff this was the final
  /// owner return and an activated recovery task should now be tombstoned.
  /// If final_return_deleted is non-null, it is set when this deletion removed
  /// the final tracked owner return regardless of whether recovery was activated.
  bool HandleOwnerReturnRefDeleted(const ObjectID &object_id,
                                   bool *final_return_deleted = nullptr);

  /// Records a received TaskSpec and returns candidate reports that should be
  /// sent to the coordinators of the received task and its dependencies.
  std::vector<CandidateReport> RegisterExecutorTask(const rpc::TaskSpec &task_spec);

  /// Records metadata for an object borrowed by this worker.
  void RegisterBorrowedObject(const ObjectID &object_id,
                              const rpc::RecoveryObjectMetadata &metadata);

  /// Prepares a provisional holder admission on the current coordinator.
  rpc::ReportRecoveryCandidateReply::Result PrepareHolderAdmission(
      const rpc::ReportRecoveryCandidateRequest &request,
      const rpc::TaskSpec *owner_task_spec,
      HolderAdmissionPlan *plan,
      rpc::RecoveryManifest *latest_manifest);

  /// Stores lineage provisionally on a candidate holder.
  bool InstallRecoveryHolder(const rpc::InstallRecoveryHolderRequest &request);

  /// Commits a previously reserved holder admission on the coordinator.
  bool CommitHolderAdmission(const std::string &reservation_id,
                             rpc::RecoveryManifest *committed_manifest);

  /// Removes a failed provisional reservation. Patch 4D removes the
  /// speculative suffix; Patch 4M-CERT removes only the failed independent
  /// certificate reservation.
  void AbortHolderAdmission(const std::string &reservation_id);

  /// Allows a borrower whose candidate report was rejected/aborted to
  /// report itself again on a later ObjectRef delivery.
  void AllowCandidateReportRetry(const TaskID &task_id);

  /// Applies a committed manifest received from the coordinator.
  bool ApplyCommittedManifest(const rpc::RecoveryManifest &manifest);

  /// Metadata lookup used by CoreWorker. The non-const overload first drives
  /// any pending adaptive Frontier recipe suffix to the established holders,
  /// then delegates to the existing const state lookup below.
  bool PopulateRecoveryMetadata(const ObjectID &object_id,
                                rpc::RecoveryObjectMetadata *metadata);

  /// Copies known recovery metadata without performing publication. Retained
  /// for manager-internal/standalone state inspection.
  bool PopulateRecoveryMetadata(const ObjectID &object_id,
                                rpc::RecoveryObjectMetadata *metadata) const;

  /// Patch 4H no-copy fast path used by task-argument activation. The
  /// non-const overload drives an adaptive suffix when necessary.
  bool HasRecoveryMetadata(const ObjectID &object_id);

  /// State-only no-copy lookup used by the adaptive wrapper and const callers.
  bool HasRecoveryMetadata(const ObjectID &object_id) const;

  /// Adds recovery metadata to direct and nested ObjectRef arguments.
  void PopulateTaskArgumentMetadata(rpc::TaskSpec *task_spec);

  /// Builds the same compact argument sidecar for a task that is still local
  /// and whose remote dispatch is gated on Recovery Frontier durability. The
  /// sidecar may describe a staged/uncommitted frontier member, but it must
  /// never leave this CoreWorker until the corresponding all-R ACK completes.
  void PopulateTaskArgumentMetadataForDeferredFrontierDispatch(
      rpc::TaskSpec *task_spec);

  struct BorrowedObjectRecoveryPlan {
    TaskID task_id;
    uint32_t return_index = 0;
    rpc::RecoveryManifest cached_manifest;
  };

  enum class ReplayPreparationResult {
    READY,
    ALREADY_OWNED,
    WITNESS_CONFIRMATION_REQUIRED,
    TASK_NOT_FOUND,
    MANIFEST_STALE,
    TOMBSTONED,
    RETRY_LIMIT_EXCEEDED,
    WRONG_HOLDER,
  };

  bool GetBorrowedObjectRecoveryPlan(const ObjectID &object_id,
                                     BorrowedObjectRecoveryPlan *plan) const;

  ReplayPreparationResult PrepareTaskReplay(
      const rpc::RecoverTaskOutputRequest &request,
      const rpc::TaskSpec *owner_task_spec,
      rpc::TaskSpec *task_spec,
      rpc::RecoveryManifest *latest_manifest);

  /// Promotes a provisional holder only from a manifest obtained directly
  /// from one of the task's compact witnesses. For an adaptive Recovery
  /// Frontier member, the witness record is group-keyed and is translated back
  /// into the requested task's member manifest before promotion.
  ///
  /// A newer witness-backed generation may also be adopted if this worker
  /// remains in the succession list.
  bool ConfirmProvisionalHolderFromWitness(
      const TaskID &task_id,
      const rpc::RecoveryManifest &witness_manifest,
      rpc::RecoveryManifest *confirmed_manifest);

  void UpdateBorrowedObjectManifest(const ObjectID &object_id,
                                    const rpc::RecoveryManifest &manifest);

  std::optional<rpc::RecoveryManifest> BuildTombstoneForTask(
      const TaskID &task_id) const;

  /// Applies a tombstone and removes retained lineage and object metadata.
  bool ApplyRecoveryTombstone(const rpc::RecoveryManifest &tombstone);

  /// Records definitive Ray failure notifications.
  void HandleWorkerFailure(const WorkerID &worker_id);
  void HandleNodeFailure(const NodeID &node_id);

  /// Returns true when the holder's worker or node is known to be dead.
  bool IsRecoveryHolderKnownFailed(const rpc::RecoveryHolder &holder) const;

  /// Returns whether this worker is a committed non-owner lineage holder.
  bool HasConfirmedHolderResponsibilities() const;

 private:
  void EraseHolderReservationLocked(const std::string &reservation_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void EraseTaskObjectMetadataLocked(const TaskID &task_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Prevalidated production owner-registration path. The caller has already
  /// checked eligibility and parsed task_id; exactly one manager lock protects
  /// the planner mutation.
  std::optional<RecoveryFrontierMembership>
  RegisterOwnerTaskWithRecoveryFrontierLocked(
      const TaskSpecification &task_spec, const TaskID &task_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Cached immutable configuration predicate used by the adaptive hot path.
  bool AdaptiveRecoveryFrontierEnabledCached() const {
    return recovery_succession_enabled_config_ &&
           !recovery_witness_holder_baseline_enabled_config_ &&
           recovery_frontier_enabled_config_ &&
           recovery_frontier_group_size_config_ > 1;
  }

  // Full initial groups can carry provisional recipes on owner exports.
  // Positive configured R/W and ordinary witness-backed admission are required.
  bool InitialFrontierPiggybackEnabledCached() const {
    return AdaptiveRecoveryFrontierEnabledCached() &&
           recovery_succession_target_holder_count_config_ > 0 &&
           recovery_succession_witness_count_config_ > 0 &&
           !recovery_succession_certificate_admission_enabled_config_;
  }

  bool StoreInitialFrontierPiggybackLocked(
      const std::string &serialized_snapshot,
      const rpc::RecoveryManifest &manifest,
      const rpc::Address &sender)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// If object_id is a member appended after the adaptive H1..HR topology was
  /// established, synchronously publish the next contiguous recipe suffix and
  /// advance the owner prefix. Returns true when the requested member is
  /// committed (including when another concurrent exporter committed it).
  bool PublishAdaptiveRecoveryFrontierForObjectIfNeeded(
      const ObjectID &object_id);

  // Patch 4J: reconstruct ordinary RecoveryObjectMetadata from task-level
  // state. object_recovery_metadata_ is only a legacy compatibility fallback.
  bool BuildRecoveryMetadataLocked(
      const ObjectID &object_id,
      rpc::RecoveryObjectMetadata *metadata,
      bool require_frontier_commit) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void PopulateTaskArgumentMetadataInternal(
      rpc::TaskSpec *task_spec, bool require_frontier_commit);

  // Optional-like storage keeps every reader on the same immutable recipe
  // interface. Replay/admission already copy and attach the current manifest.
  class RetainedTaskRecipe {
   public:
    bool has_value() const { return shared_ != nullptr || owned_.has_value(); }
    const rpc::TaskSpec &value() const { return shared_ ? *shared_ : owned_.value(); }
    const rpc::TaskSpec *operator->() const { return &value(); }
    void reset() { owned_.reset(); shared_.reset(); }
    RetainedTaskRecipe &operator=(rpc::TaskSpec recipe) {
      shared_.reset();
      owned_ = std::move(recipe);
      return *this;
    }
    void Share(std::shared_ptr<const rpc::TaskSpec> recipe) {
      owned_.reset();
      shared_ = std::move(recipe);
    }
    bool IsShared() const { return shared_ != nullptr; }

   private:
    std::optional<rpc::TaskSpec> owned_;
    std::shared_ptr<const rpc::TaskSpec> shared_;
  };

  struct TaskRecoveryState {
    rpc::RecoveryManifest manifest;

    // Patch 4J: static return count lets the owner reconstruct per-object
    // metadata from ObjectID::ObjectIndex() without a per-return manifest copy.
    uint32_t owned_num_returns = 0;

    // Present on executors and installed/piggyback lineage holders. The
    // original owner may leave this empty and use TaskManager on demand.
    RetainedTaskRecipe task_spec;

    // An installed holder is not usable until either CommitRecoveryManifest
    // arrives or the holder independently confirms the manifest from a compact
    // witness during owner-failure recovery.
    bool manifest_committed = true;
    std::string provisional_reservation_id;

    // Transport attempts, not distinct/installed holders. After R successful
    // piggyback serializations, later candidates use the ordinary install path.
    uint32_t first_holder_piggybacks_sent = 0;

    // Borrower-side state: full TaskSpec is present, but replay remains blocked
    // until a witness-confirmed manifest explicitly contains this worker.
    bool provisional_piggyback_task_spec = false;
  };

  struct OwnerRetainedTaskState {
    // Production adaptive Succession leaves this empty because TaskManager owns
    // the sole dormant recipe. Direct manager tests/non-CoreWorker callers may
    // use this compatibility fallback.
    rpc::TaskSpec task_spec;
    uint64_t task_spec_bytes = 0;

    // Keep the original set for Fixed-R and legacy/direct-manager paths so this
    // optimization does not change the baseline. Adaptive Succession uses only
    // remaining_live_returns and avoids allocating/hashing the set.
    absl::flat_hash_set<ObjectID> live_return_ids;
    uint32_t remaining_live_returns = 0;

    bool HasLiveReturns() const {
      return remaining_live_returns > 0 || !live_return_ids.empty();
    }
  };

  void StoreFrontierHolderRecipeLocked(
      const std::shared_ptr<const rpc::TaskSpec> &recipe,
      const rpc::RecoveryManifest &manifest,
      TaskRecoveryState *state) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  struct BorrowedObjectRecoveryState {
    TaskID task_id;
    uint32_t return_index = 0;
  };

  struct HolderReservation {
    TaskID task_id;
    rpc::Address candidate_address;
    rpc::RecoveryManifest proposed_manifest;
    // Patch 4M-CERT: in certificate mode this is an owner-issued admission
    // slot/token. The committed recovery rank is derived later from the merged set.
    uint32_t proposed_rank = 0;
  };

  void MaybeAddCandidateReportLocked(const rpc::RecoveryManifest &manifest,
                                     bool already_stores_task_spec,
                                     std::vector<CandidateReport> *reports)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void UpdateManifestForTaskLocked(const TaskID &task_id,
                                   const rpc::RecoveryManifest &manifest,
                                   bool committed) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  inline static std::atomic<RecoverySuccessionManager *> process_recovery_manager_{
      nullptr};

  struct ProcessRecoveryManagerRegistration {
    explicit ProcessRecoveryManagerRegistration(RecoverySuccessionManager *manager_ptr)
        : registered_manager_(manager_ptr) {
      process_recovery_manager_.store(manager_ptr, std::memory_order_release);
    }

    ~ProcessRecoveryManagerRegistration() {
      RecoverySuccessionManager *expected = registered_manager_;
      process_recovery_manager_.compare_exchange_strong(
          expected, nullptr, std::memory_order_acq_rel);
    }

    RecoverySuccessionManager *registered_manager_;
  };

  /// Address of this CoreWorker.
  rpc::Address self_address_;

  const bool profiling_enabled_;

  // Ray system_config is fixed for the CoreWorker lifetime. Cache the flags
  // touched by per-task Recovery Succession paths instead of repeatedly
  // traversing RayConfig/string-backed configuration state.
  const bool recovery_succession_enabled_config_;
  const bool shared_holder_recipe_enabled_config_;
  const bool recovery_frontier_enabled_config_;
  const uint32_t recovery_frontier_group_size_config_;
  const bool recovery_witness_holder_baseline_enabled_config_;
  const bool recovery_succession_certificate_admission_enabled_config_;
  const bool recovery_succession_task_manager_pin_enabled_config_;
  const uint32_t recovery_succession_target_holder_count_config_;
  const uint32_t recovery_succession_witness_count_config_;

  mutable absl::Mutex mutex_;

  /// Backend-neutral owner-side grouping state. Null when Recovery Frontiers
  /// are disabled. All mutable planner access is serialized by the manager
  /// mutex; immutable enable/group-size decisions use the cached fields above.
  std::unique_ptr<RecoveryFrontierPlanner> recovery_frontier_planner_
      ABSL_GUARDED_BY(mutex_);

  /// Immutable backend topology for each activated frontier group. The replay
  /// capsule grows, but its fixed-R witness/holder set never changes.
  absl::flat_hash_map<TaskID, rpc::RecoveryManifest>
      recovery_frontier_protection_manifests_ ABSL_GUARDED_BY(mutex_);

  /// Exact initial recipe prefix frozen for the duration of adaptive holder
  /// admission. The Frontier itself remains open so later owner tasks can join
  /// it; H1..HR nevertheless all receive the same initial replay snapshot.
  absl::flat_hash_map<TaskID, RecoveryFrontierAppendBatch>
      adaptive_frontier_initial_append_batches_ ABSL_GUARDED_BY(mutex_);

  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);

  /// Recovery state indexed by the original task ID.
  absl::flat_hash_map<TaskID, TaskRecoveryState> task_states_ ABSL_GUARDED_BY(mutex_);

  /// Patch 4L: one correctness-preserving owner TaskSpec copy retained
  /// independently of TaskManager's ordinary lineage lifetime. Presence here
  /// does not mean recovery has been activated.
  absl::flat_hash_map<TaskID, OwnerRetainedTaskState> owner_retained_tasks_
      ABSL_GUARDED_BY(mutex_);

  /// Recovery information for borrowed objects.
  absl::flat_hash_map<ObjectID, BorrowedObjectRecoveryState> borrowed_objects_
      ABSL_GUARDED_BY(mutex_);

  /// Metadata attached whenever an ObjectRef is serialized.
  absl::flat_hash_map<ObjectID, rpc::RecoveryObjectMetadata> object_recovery_metadata_
      ABSL_GUARDED_BY(mutex_);

  /// Object IDs carrying recovery metadata, indexed by producer task.
  /// This avoids scanning every tracked object when one task's manifest changes
  /// or its tombstone is applied.
  absl::flat_hash_map<TaskID, absl::flat_hash_set<ObjectID>> task_object_ids_
      ABSL_GUARDED_BY(mutex_);

  /// Provisional owner-side holder reservations.
  absl::flat_hash_map<std::string, HolderReservation> holder_reservations_
      ABSL_GUARDED_BY(mutex_);

  /// Patch 4D: multiple provisional reservations may coexist per task.
  /// The ordered rank map is the speculative prefix H1..HR.
  absl::flat_hash_map<TaskID, std::map<uint32_t, std::string>>
      holder_reservation_by_task_ ABSL_GUARDED_BY(mutex_);

  /// Prevents repeated reports for the same producer task.
  absl::flat_hash_set<TaskID> candidate_reports_sent_ ABSL_GUARDED_BY(mutex_);

  /// Workers definitively reported dead by the GCS.
  absl::flat_hash_set<WorkerID> failed_workers_ ABSL_GUARDED_BY(mutex_);

  /// Nodes definitively reported dead by the GCS.
  absl::flat_hash_set<NodeID> failed_nodes_ ABSL_GUARDED_BY(mutex_);

  // Declared last so its destructor clears the process pointer before the
  // manager's state begins destruction.
  ProcessRecoveryManagerRegistration process_registration_{this};
};

}  // namespace ray::core