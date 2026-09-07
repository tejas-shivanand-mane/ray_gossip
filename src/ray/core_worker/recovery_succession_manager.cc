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

#include "ray/core_worker/recovery_succession_manager.h"
#include "ray/core_worker/recovery_succession_manager_internal.h"
#include "ray/common/ray_config.h"
#include "absl/cleanup/cleanup.h"
#include <algorithm>
#include <cstddef>
#include <utility>
#include <chrono>


namespace ray::core {

// Patch 4D: pipelined holder admission.
// Patch 4F: first-holder TaskSpec piggyback.
// Patch 4G: hot-path profiling.
// Patch 4H: compact task-argument recovery metadata.
// Patch 4I: TaskSpec-level recovery argument sidecar.
// Patch 4J: task-centric recovery state.
// Patch 4K: full mode uses async holder install; no H1 TaskSpec piggyback.
// Patch 4L: retain one owner TaskSpec copy until returned refs truly die.

namespace {


const rpc::RecoveryHolder *FindHolderByRank(const rpc::RecoveryManifest &manifest,
                                            uint32_t rank) {
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (holder.rank() == rank) {
      return &holder;
    }
  }

  return nullptr;
}

bool SameWorker(const rpc::Address &left, const rpc::Address &right) {
  return !left.worker_id().empty() && left.worker_id() == right.worker_id();
}

bool ContainsWorker(const rpc::RecoveryManifest &manifest, const rpc::Address &address) {
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (SameWorker(holder.address(), address)) {
      return true;
    }
  }

  return false;
}

int CompareManifestVersions(const rpc::RecoveryManifest &left,
                            const rpc::RecoveryManifest &right) {
  if (left.version().generation() < right.version().generation()) {
    return -1;
  }

  if (left.version().generation() > right.version().generation()) {
    return 1;
  }

  return 0;
}


bool MergeRecoveryHolderSets(const rpc::RecoveryManifest &incoming,
                             rpc::RecoveryManifest *state) {
  if (state == nullptr || incoming.task_id().empty() || !incoming.has_version()) {
    return false;
  }
  if (state->task_id().empty()) {
    state->CopyFrom(incoming);
    return true;
  }
  if (state->task_id() != incoming.task_id() || !state->has_version()) {
    return false;
  }

  // Tombstones are terminal.  An equal-generation tombstone may race with an
  // independently published certificate; terminal state wins that tie.
  if (incoming.tombstoned()) {
    if (incoming.version().generation() >= state->version().generation()) {
      state->CopyFrom(incoming);
      return true;
    }
    return false;
  }
  if (state->tombstoned()) {
    return true;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(*state, 0);
  if (owner == nullptr) {
    owner = FindHolderByRank(incoming, 0);
  }
  if (owner == nullptr) {
    return false;
  }

  std::vector<rpc::RecoveryHolder> holders;
  auto add_unique = [&holders, owner](const rpc::RecoveryManifest &manifest) {
    for (const rpc::RecoveryHolder &holder : manifest.succession()) {
      if (holder.rank() == 0 || SameWorker(holder.address(), owner->address())) {
        continue;
      }
      bool duplicate = false;
      for (const rpc::RecoveryHolder &existing : holders) {
        if (SameWorker(existing.address(), holder.address())) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        holders.push_back(holder);
      }
    }
  };

  add_unique(*state);
  add_unique(incoming);
  if (holders.size() > static_cast<size_t>(state->target_holder_count())) {
    return false;
  }

  std::sort(holders.begin(), holders.end(),
            [](const rpc::RecoveryHolder &a, const rpc::RecoveryHolder &b) {
              return a.address().worker_id() < b.address().worker_id();
            });

  rpc::RecoveryHolder owner_copy;
  owner_copy.CopyFrom(*owner);
  state->clear_succession();
  rpc::RecoveryHolder *out_owner = state->add_succession();
  out_owner->CopyFrom(owner_copy);
  out_owner->set_rank(0);
  for (size_t i = 0; i < holders.size(); ++i) {
    rpc::RecoveryHolder *out = state->add_succession();
    out->CopyFrom(holders[i]);
    out->set_rank(static_cast<uint32_t>(i + 1));
  }

  state->mutable_version()->set_generation(
      std::max(state->version().generation(), incoming.version().generation()));
  state->set_recovery_attempt(
      std::max(state->recovery_attempt(), incoming.recovery_attempt()));
  state->set_frozen(static_cast<uint32_t>(holders.size()) >=
                    state->target_holder_count());
  return true;
}

bool MergeConfirmedHolder(const rpc::RecoveryHolder &candidate,
                          uint64_t certificate_generation,
                          rpc::RecoveryManifest *manifest) {
  if (manifest == nullptr || manifest->task_id().empty() || manifest->tombstoned() ||
      candidate.address().worker_id().empty()) {
    return false;
  }

  rpc::RecoveryManifest delta;
  delta.CopyFrom(*manifest);
  delta.clear_succession();

  const rpc::RecoveryHolder *owner = FindHolderByRank(*manifest, 0);
  if (owner == nullptr) {
    return false;
  }
  delta.add_succession()->CopyFrom(*owner);
  rpc::RecoveryHolder *holder = delta.add_succession();
  holder->CopyFrom(candidate);
  holder->set_rank(1);
  delta.mutable_version()->set_generation(certificate_generation);
  return MergeRecoveryHolderSets(delta, manifest);
}


// Patch 4F transport sidecars must never become part of retained/replayed
// lineage, otherwise a downstream TaskSpec could recursively contain upstream
// full TaskSpecs.
void ClearFirstHolderTaskSpecPiggybacks(rpc::TaskSpec *task_spec) {
  if (task_spec == nullptr) {
    return;
  }

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      arg.mutable_object_ref()
          ->mutable_recovery_metadata()
          ->clear_first_holder_task_spec();
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        nested_ref.mutable_recovery_metadata()->clear_first_holder_task_spec();
      }
    }
  }

  // Patch 4I sidecars are part of the downstream TaskSpec's dependency
  // recovery description and therefore must survive replay. Only the nested
  // full-lineage piggyback is transport-only and must be stripped.
  for (rpc::RecoveryTaskArgumentMetadata &entry :
       *task_spec->mutable_recovery_argument_metadata()) {
    entry.clear_initial_frontier_recipe();
    if (entry.has_recovery_metadata()) {
      entry.mutable_recovery_metadata()->clear_first_holder_task_spec();
    }
  }
}


// Patch 4H sender-side compact encoding. This is intentionally restricted to
// TaskSpec argument transport; internal recovery state still stores a complete
// RecoveryManifest. If the standard ObjectReference owner cannot reproduce the
// manifest's rank-0 owner exactly enough for recovery, return false and the
// caller falls back to the old full metadata representation.
bool WriteCompactTaskArgumentRecoveryMetadata(
    const rpc::RecoveryObjectMetadata &source,
    const rpc::RecoveryManifest &manifest,
    const rpc::Address &object_owner,
    rpc::RecoveryObjectMetadata *out) {
  if (out == nullptr || source.task_id().empty() || manifest.task_id().empty() ||
      source.task_id() != manifest.task_id() || !manifest.has_version()) {
    return false;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(manifest, 0);
  if (owner == nullptr || object_owner.worker_id().empty() ||
      !SameWorker(owner->address(), object_owner)) {
    return false;
  }

  out->Clear();
  out->set_return_index(source.return_index());

  rpc::RecoveryObjectTransportManifest *compact = out->mutable_compact_manifest();
  compact->set_target_holder_count(manifest.target_holder_count());
  compact->set_witness_count(manifest.witness_count());
  compact->set_generation(manifest.version().generation());
  compact->set_frozen(manifest.frozen());
  compact->set_tombstoned(manifest.tombstoned());
  compact->set_recovery_attempt(manifest.recovery_attempt());
  compact->set_max_recovery_attempts(manifest.max_recovery_attempts());

  for (const rpc::Address &witness : manifest.witness_raylets()) {
    compact->add_witness_raylets()->CopyFrom(witness);
  }

  // Ranks are fixed and contiguous in Recovery Succession. Rank 0 is already
  // carried by ObjectReference.owner_address, so only H1..HR are transported.
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (holder.rank() == 0) {
      continue;
    }
    if (holder.rank() !=
        static_cast<uint32_t>(compact->non_owner_holders_size() + 1)) {
      out->Clear();
      return false;
    }
    compact->add_non_owner_holders()->CopyFrom(holder.address());
  }

  return true;
}

// Patch 4H receiver-side expansion. Existing admission, witness confirmation,
// replay, tombstone, and rollback code continues to consume the ordinary full
// RecoveryManifest, so the compact representation never escapes this boundary.
bool ExpandTaskArgumentRecoveryMetadata(
    const rpc::ObjectReference &object_ref,
    rpc::RecoveryObjectMetadata *expanded) {
  if (expanded == nullptr || object_ref.object_id().empty() ||
      !object_ref.has_recovery_metadata()) {
    return false;
  }

  const rpc::RecoveryObjectMetadata &transport = object_ref.recovery_metadata();

  // Backward-compatible/fallback path.
  if (!transport.task_id().empty() && transport.has_manifest()) {
    expanded->CopyFrom(transport);
    expanded->clear_compact_manifest();
    return true;
  }

  if (!transport.has_compact_manifest() || !object_ref.has_owner_address() ||
      object_ref.owner_address().worker_id().empty() ||
      object_ref.object_id().size() != ObjectID::Size()) {
    return false;
  }

  const rpc::RecoveryObjectTransportManifest &compact =
      transport.compact_manifest();
  if (compact.generation() == 0) {
    return false;
  }

  const ObjectID object_id = ObjectID::FromBinary(object_ref.object_id());
  const TaskID task_id = object_id.TaskId();

  expanded->Clear();
  expanded->set_task_id(task_id.Binary());
  expanded->set_return_index(transport.return_index());
  if (!transport.first_holder_task_spec().empty()) {
    expanded->set_first_holder_task_spec(transport.first_holder_task_spec());
  }

  rpc::RecoveryManifest *manifest = expanded->mutable_manifest();
  manifest->set_task_id(task_id.Binary());
  manifest->set_job_id(task_id.JobId().Binary());
  manifest->set_target_holder_count(compact.target_holder_count());
  manifest->set_witness_count(compact.witness_count());
  manifest->mutable_version()->set_generation(compact.generation());
  manifest->set_frozen(compact.frozen());
  manifest->set_tombstoned(compact.tombstoned());
  manifest->set_recovery_attempt(compact.recovery_attempt());
  manifest->set_max_recovery_attempts(compact.max_recovery_attempts());

  for (const rpc::Address &witness : compact.witness_raylets()) {
    manifest->add_witness_raylets()->CopyFrom(witness);
  }

  rpc::RecoveryHolder *owner = manifest->add_succession();
  owner->mutable_address()->CopyFrom(object_ref.owner_address());
  owner->set_rank(0);
  owner->set_failure_domain_id(object_ref.owner_address().node_id());

  for (int i = 0; i < compact.non_owner_holders_size(); ++i) {
    const rpc::Address &address = compact.non_owner_holders(i);
    if (address.worker_id().empty()) {
      expanded->Clear();
      return false;
    }
    rpc::RecoveryHolder *holder = manifest->add_succession();
    holder->mutable_address()->CopyFrom(address);
    holder->set_rank(static_cast<uint32_t>(i + 1));
    holder->set_failure_domain_id(address.node_id());
  }

  return true;
}


// Patch 4I TaskSpec-level sidecar expansion. Reuse the Patch-4H expansion
// logic through a local synthetic ObjectReference so all downstream manager
// state continues to see the exact ordinary RecoveryObjectMetadata shape.
bool ExpandTaskSidecarRecoveryMetadata(
    const rpc::RecoveryTaskArgumentMetadata &entry,
    rpc::RecoveryObjectMetadata *expanded) {
  if (expanded == nullptr || entry.object_id().empty() ||
      !entry.has_recovery_metadata()) {
    return false;
  }

  rpc::ObjectReference synthetic_ref;
  synthetic_ref.set_object_id(entry.object_id());
  if (entry.has_owner_address()) {
    synthetic_ref.mutable_owner_address()->CopyFrom(entry.owner_address());
  }
  synthetic_ref.mutable_recovery_metadata()->CopyFrom(entry.recovery_metadata());
  return ExpandTaskArgumentRecoveryMetadata(synthetic_ref, expanded);
}

}  // namespace

RecoverySuccessionManager::RecoverySuccessionManager(rpc::Address self_address)
    : self_address_(std::move(self_address)),
      profiling_enabled_(
          RayConfig::instance().enable_recovery_succession_profiling()),
      recovery_succession_enabled_config_(
          RayConfig::instance().enable_recovery_succession()),
      shared_holder_recipe_enabled_config_(
          RayConfig::instance().enable_recovery_succession_shared_holder_recipe()),
      recovery_frontier_enabled_config_(
          RayConfig::instance().enable_recovery_frontier()),
      recovery_frontier_group_size_config_(
          RayConfig::instance().recovery_frontier_group_size()),
      recovery_witness_holder_baseline_enabled_config_(
          RayConfig::instance().enable_recovery_witness_holder_baseline()),
      recovery_succession_certificate_admission_enabled_config_(
          RayConfig::instance().enable_recovery_succession_certificate_admission()),
      recovery_succession_task_manager_pin_enabled_config_(
          RayConfig::instance().enable_recovery_succession_task_manager_pin()),
      recovery_succession_target_holder_count_config_(
          RayConfig::instance().recovery_succession_target_holder_count()),
      recovery_succession_witness_count_config_(
          RayConfig::instance().recovery_succession_witness_count()) {
  if (recovery_frontier_enabled_config_) {
    RAY_CHECK_GT(recovery_frontier_group_size_config_, 0U)
        << "recovery_frontier_group_size must be positive";
    recovery_frontier_planner_ =
        std::make_unique<RecoveryFrontierPlanner>(
            recovery_frontier_group_size_config_);
  }
}

bool RecoverySuccessionManager::IsEligibleTask(const rpc::TaskSpec &task_spec) {
  return task_spec.type() == rpc::TaskType::NORMAL_TASK && !task_spec.returns_dynamic() &&
         !task_spec.streaming_generator() && task_spec.max_retries() != 0;
}

void RecoverySuccessionManager::StoreFrontierHolderRecipeLocked(
    const std::shared_ptr<const rpc::TaskSpec> &recipe,
    const rpc::RecoveryManifest &manifest,
    TaskRecoveryState *state) {
  RAY_CHECK(recipe != nullptr && state != nullptr);
  bool share = shared_holder_recipe_enabled_config_ &&
               AdaptiveRecoveryFrontierEnabledCached();
  if (share) {
    // A shared recipe must already be sanitized. Never mutate the group copy,
    // and never retain nested transport-only lineage through this shortcut.
    const auto has_payload = [](const rpc::ObjectReference &ref) {
      return ref.has_recovery_metadata() &&
             !ref.recovery_metadata().first_holder_task_spec().empty();
    };
    for (const auto &arg : recipe->args()) {
      if (arg.has_object_ref() && has_payload(arg.object_ref())) {
        share = false;
      }
      for (const auto &ref : arg.nested_inlined_refs()) {
        if (has_payload(ref)) {
          share = false;
        }
      }
    }
    for (const auto &entry : recipe->recovery_argument_metadata()) {
      if (!entry.initial_frontier_recipe().empty() ||
          (entry.has_recovery_metadata() &&
           !entry.recovery_metadata().first_holder_task_spec().empty())) {
        share = false;
      }
    }
  }
  if (share) {
    state->task_spec.Share(recipe);
    if (profiling_enabled_) {
      ++profile_.holder_recipe_copies_avoided;
    }
    return;
  }

  rpc::TaskSpec copy;
  copy.CopyFrom(*recipe);
  ClearFirstHolderTaskSpecPiggybacks(&copy);
  copy.mutable_recovery_manifest()->CopyFrom(manifest);
  state->task_spec = std::move(copy);
  if (profiling_enabled_) {
    ++profile_.holder_recipe_fallback_copies;
  }
}

bool RecoverySuccessionManager::RecoveryFrontierEnabled() const {
  return recovery_frontier_enabled_config_;
}

std::optional<RecoveryFrontierMembership>
RecoverySuccessionManager::RegisterOwnerTaskWithRecoveryFrontierLocked(
    const TaskSpecification &task_spec, const TaskID &task_id) {
  if (!recovery_frontier_enabled_config_ ||
      recovery_frontier_planner_ == nullptr || task_id.IsNil()) {
    return std::nullopt;
  }
  return recovery_frontier_planner_->RegisterTask(task_spec.GetSharedMessage());
}

std::optional<RecoveryFrontierMembership>
RecoverySuccessionManager::RegisterOwnerTaskWithRecoveryFrontier(
    const TaskSpecification &task_spec) {
  if (!recovery_frontier_enabled_config_) {
    return std::nullopt;
  }

  const rpc::TaskSpec &task_proto = task_spec.GetMessage();
  if (!IsEligibleTask(task_proto) || task_proto.task_id().empty() ||
      task_spec.NumReturns() == 0) {
    return std::nullopt;
  }

  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());
  absl::MutexLock lock(&mutex_);
  return RegisterOwnerTaskWithRecoveryFrontierLocked(task_spec, task_id);
}

std::optional<RecoveryFrontierMembership>
RecoverySuccessionManager::GetRecoveryFrontierMembership(
    const TaskID &task_id) const {
  if (!recovery_frontier_enabled_config_) {
    return std::nullopt;
  }
  absl::MutexLock lock(&mutex_);
  if (recovery_frontier_planner_ == nullptr) {
    return std::nullopt;
  }
  return recovery_frontier_planner_->FindTask(task_id);
}

bool RecoverySuccessionManager::GetRecoveryFrontierProtectionManifest(
    const TaskID &group_id, rpc::RecoveryManifest *manifest) const {
  if (group_id.IsNil() || manifest == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);
  const auto it = recovery_frontier_protection_manifests_.find(group_id);
  if (it == recovery_frontier_protection_manifests_.end()) {
    return false;
  }
  manifest->CopyFrom(it->second);
  return true;
}

bool RecoverySuccessionManager::CacheRecoveryFrontierProtectionManifest(
    const rpc::RecoveryManifest &candidate,
    rpc::RecoveryManifest *authoritative_manifest) {
  if (authoritative_manifest == nullptr ||
      candidate.task_id().size() != TaskID::Size()) {
    return false;
  }

  const TaskID group_id = TaskID::FromBinary(candidate.task_id());
  absl::MutexLock lock(&mutex_);
  if (recovery_frontier_planner_ == nullptr ||
      recovery_frontier_planner_->GetGroup(group_id) == nullptr) {
    return false;
  }

  auto [it, inserted] =
      recovery_frontier_protection_manifests_.try_emplace(group_id);
  if (inserted) {
    it->second.CopyFrom(candidate);
  }
  authoritative_manifest->CopyFrom(it->second);
  return true;
}

std::optional<RecoveryFrontierAppendBatch>
RecoverySuccessionManager::StageRecoveryFrontierAppend(
    const TaskID &group_id, uint32_t max_batch_members) {
  absl::MutexLock lock(&mutex_);
  if (recovery_frontier_planner_ == nullptr) {
    return std::nullopt;
  }
  RecoveryFrontierGroup *group =
      recovery_frontier_planner_->GetMutableGroup(group_id);
  return group == nullptr ? std::nullopt : group->StageAppend(max_batch_members);
}

bool RecoverySuccessionManager::RecoveryFrontierGroupHasUncommittedMembers(
    const TaskID &group_id) const {
  if (group_id.IsNil()) {
    return false;
  }

  absl::MutexLock lock(&mutex_);
  if (recovery_frontier_planner_ == nullptr) {
    return false;
  }
  const RecoveryFrontierGroup *group =
      recovery_frontier_planner_->GetGroup(group_id);
  return group != nullptr && group->HasUncommittedMembers();
}

bool RecoverySuccessionManager::CommitRecoveryFrontierAppend(
    const RecoveryFrontierAppendBatch &batch) {
  absl::MutexLock lock(&mutex_);
  if (recovery_frontier_planner_ == nullptr) {
    return false;
  }
  RecoveryFrontierGroup *group =
      recovery_frontier_planner_->GetMutableGroup(batch.group_id);
  return group != nullptr && group->CommitAppend(batch);
}

bool RecoverySuccessionManager::AbortRecoveryFrontierAppend(
    const RecoveryFrontierAppendBatch &batch) {
  absl::MutexLock lock(&mutex_);
  if (recovery_frontier_planner_ == nullptr) {
    return false;
  }
  RecoveryFrontierGroup *group =
      recovery_frontier_planner_->GetMutableGroup(batch.group_id);
  return group != nullptr && group->AbortAppend(batch);
}

bool RecoverySuccessionManager::BuildRecoveryFrontierAppendProto(
    const RecoveryFrontierAppendBatch &batch,
    rpc::RecoveryFrontierAppend *append) {
  return recovery_succession_internal::BuildFrontierSuccessionAppend(
      batch, append);
}

bool RecoverySuccessionManager::CommitAdaptiveRecoveryFrontierAppend(
    const RecoveryFrontierAppendBatch &batch,
    const rpc::RecoveryManifest &group_manifest) {
  if (batch.group_id.IsNil() ||
      group_manifest.task_id() != batch.group_id.Binary() ||
      !group_manifest.frozen() ||
      !ContainsWorker(group_manifest, self_address_)) {
    return false;
  }

  absl::MutexLock lock(&mutex_);
  if (!AdaptiveRecoveryFrontierEnabledCached()) {
    return false;
  }

  RecoveryFrontierGroup *group =
      recovery_frontier_planner_->GetMutableGroup(batch.group_id);
  if (group == nullptr || !group->CommitAppend(batch)) {
    return false;
  }

  recovery_frontier_protection_manifests_[batch.group_id].CopyFrom(
      group_manifest);

  for (const RecoveryFrontierMember &member : batch.members) {
    if (member.task_spec == nullptr ||
        !group->IsTaskCommitted(member.task_id)) {
      return false;
    }
    rpc::RecoveryManifest member_manifest =
        recovery_succession_internal::BuildFrontierMemberManifest(
            group_manifest, member);
    UpdateManifestForTaskLocked(member.task_id, member_manifest, true);
  }

  return true;
}

bool RecoverySuccessionManager::ApplyAdaptiveRecoveryFrontierAppend(
    const rpc::RecoveryFrontierAppend &append,
    const rpc::RecoveryManifest &group_manifest) {
  if (append.group_id().size() != TaskID::Size() ||
      group_manifest.task_id() != append.group_id() ||
      !group_manifest.frozen() ||
      !ContainsWorker(group_manifest, self_address_) ||
      append.members_size() <= 0) {
    return false;
  }

  for (const rpc::RecoveryFrontierMemberRecord &record : append.members()) {
    if (!record.has_task_spec() ||
        record.task_spec().task_id() != record.task_id() ||
        !IsEligibleTask(record.task_spec())) {
      return false;
    }
  }

  const TaskID group_id = TaskID::FromBinary(append.group_id());
  absl::MutexLock lock(&mutex_);
  if (!AdaptiveRecoveryFrontierEnabledCached() ||
      !recovery_frontier_planner_->ApplyCommittedAppend(append)) {
    return false;
  }

  const RecoveryFrontierGroup *group =
      recovery_frontier_planner_->GetGroup(group_id);
  if (group == nullptr ||
      append.end_member_index() > group->MemberCount()) {
    return false;
  }

  recovery_frontier_protection_manifests_[group_id].CopyFrom(group_manifest);

  for (uint32_t index = append.begin_member_index();
       index < append.end_member_index();
       ++index) {
    const RecoveryFrontierMember &member = group->Members()[index];
    if (member.task_spec == nullptr || !IsEligibleTask(*member.task_spec)) {
      return false;
    }

    rpc::RecoveryManifest member_manifest =
        recovery_succession_internal::BuildFrontierMemberManifest(
            group_manifest, member);

    TaskRecoveryState &member_state = task_states_[member.task_id];
    member_state.manifest.CopyFrom(member_manifest);
    StoreFrontierHolderRecipeLocked(member.task_spec, member_manifest, &member_state);
    member_state.manifest_committed = true;
    member_state.provisional_reservation_id.clear();
    member_state.provisional_piggyback_task_spec = false;
  }

  candidate_reports_sent_.insert(group_id);
  return true;
}

bool RecoverySuccessionManager::ExtractRecoveryFrontierTaskForReturn(
    const TaskID &group_id,
    uint32_t group_return_index,
    rpc::TaskSpec *task_spec,
    uint32_t *task_return_index) const {
  absl::MutexLock lock(&mutex_);
  if (recovery_frontier_planner_ == nullptr) {
    return false;
  }
  const RecoveryFrontierGroup *group =
      recovery_frontier_planner_->GetGroup(group_id);
  return group != nullptr &&
         group->ExtractTaskForReturn(
             group_return_index, task_spec, task_return_index);
}

bool RecoverySuccessionManager::CarriesRecoveryMetadata(
    const rpc::TaskSpec &task_spec) {
  if (task_spec.has_recovery_manifest() ||
      task_spec.recovery_argument_metadata_size() > 0) {
    return true;
  }

  // Backward compatibility for TaskSpecs created by pre-4I workers/tests.
  for (const rpc::TaskArg &arg : task_spec.args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      return true;
    }

    for (const rpc::ObjectReference &nested_ref : arg.nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        return true;
      }
    }
  }

  return false;
}

rpc::RecoveryManifest RecoverySuccessionManager::BuildInitialManifest(
    const TaskID &task_id, const JobID &job_id, int32_t max_retries) const {
  rpc::RecoveryManifest manifest;

  manifest.set_task_id(task_id.Binary());
  manifest.set_job_id(job_id.Binary());
  const uint32_t target_holder_count =
      recovery_succession_target_holder_count_config_;

  RAY_CHECK_GT(target_holder_count, 0U);

  manifest.set_target_holder_count(target_holder_count);

  manifest.set_witness_count(0);

  rpc::RecoveryManifestVersion *version = manifest.mutable_version();
  version->set_generation(1);

  manifest.set_frozen(false);
  manifest.set_tombstoned(false);
  manifest.set_recovery_attempt(0);
  manifest.set_max_recovery_attempts(max_retries);

  rpc::RecoveryHolder *owner = manifest.add_succession();
  owner->mutable_address()->CopyFrom(self_address_);
  owner->set_rank(0);
  owner->set_failure_domain_id(self_address_.node_id());

  return manifest;
}


void RecoverySuccessionManager::RegisterOwnedTask(
    const TaskSpecification &task_spec,
    std::vector<rpc::ObjectReference> *returned_refs) {
  if (returned_refs == nullptr) {
    return;
  }

  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  if (!task_proto.has_recovery_manifest() || task_proto.task_id().empty()) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());

  TaskRecoveryState task_state;
  task_state.manifest.CopyFrom(task_proto.recovery_manifest());
  task_state.owned_num_returns =
      static_cast<uint32_t>(task_spec.NumReturns());

  rpc::TaskSpec stored_task_spec;
  stored_task_spec.CopyFrom(task_proto);
  ClearFirstHolderTaskSpecPiggybacks(&stored_task_spec);
  stored_task_spec.mutable_recovery_manifest()->CopyFrom(
      task_proto.recovery_manifest());

  task_state.task_spec = std::move(stored_task_spec);
  task_state.manifest_committed = true;

  absl::MutexLock lock(&mutex_);
  task_states_[task_id] = std::move(task_state);

  for (size_t return_index = 0; return_index < returned_refs->size(); ++return_index) {
    rpc::ObjectReference &returned_ref = returned_refs->at(return_index);
    if (returned_ref.object_id().empty()) {
      continue;
    }

    rpc::RecoveryObjectMetadata metadata;
    metadata.set_task_id(task_proto.task_id());
    metadata.set_return_index(static_cast<uint32_t>(return_index));
    metadata.mutable_manifest()->CopyFrom(task_proto.recovery_manifest());
    returned_ref.mutable_recovery_metadata()->CopyFrom(metadata);
  }
}



bool RecoverySuccessionManager::RegisterOwnedTaskLazy(
    const TaskSpecification &task_spec,
    const rpc::RecoveryManifest &manifest) {
  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  if (task_proto.task_id().empty() || manifest.task_id().empty() ||
      task_proto.task_id() != manifest.task_id()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());

  absl::MutexLock lock(&mutex_);

  const auto existing_it = task_states_.find(task_id);
  if (existing_it != task_states_.end()) {
    if (existing_it->second.manifest.tombstoned()) {
      return false;
    }
    return false;
  }

  TaskRecoveryState task_state;
  task_state.manifest.CopyFrom(manifest);
  task_state.owned_num_returns =
      static_cast<uint32_t>(task_spec.NumReturns());
  task_state.manifest_committed = true;

  // Ordinary adaptive K=1 only: retain one short-lived producer recipe so the
  // first downstream borrower can receive H1 lineage on its existing PushTask.
  // TaskManager remains the authoritative owner recipe. Frontier keeps its
  // typed membership sidecar and Fixed-R/certificate modes are unchanged.
  if (recovery_succession_enabled_config_ &&
      !recovery_witness_holder_baseline_enabled_config_ &&
      !recovery_frontier_enabled_config_ &&
      !recovery_succession_certificate_admission_enabled_config_ &&
      !manifest.tombstoned() && !manifest.frozen() &&
      manifest.succession_size() == 1) {
    rpc::TaskSpec transient_task_spec;
    transient_task_spec.CopyFrom(task_proto);
    ClearFirstHolderTaskSpecPiggybacks(&transient_task_spec);
    transient_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);
    task_state.task_spec = std::move(transient_task_spec);
  }

  task_states_[task_id] = std::move(task_state);

  // Production adaptive owner lineage continues to live in TaskManager after
  // the one-shot H1 transport copy is released.

  return true;
}


void RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery(
    const TaskSpecification &task_spec,
    const std::vector<rpc::ObjectReference> &returned_refs,
    bool task_manager_owns_recipe) {
  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  // Production CoreWorker calls this only after the eligibility gate in
  // SubmitTask. Keep the full validation for direct-manager/test callers, but
  // do not repeat it on every production task.
  if (task_proto.task_id().empty() || task_proto.has_recovery_manifest() ||
      (!task_manager_owns_recipe && !IsEligibleTask(task_proto))) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());

  const bool baseline_enabled =
      recovery_witness_holder_baseline_enabled_config_;
  const bool production_adaptive =
      task_manager_owns_recipe && recovery_succession_enabled_config_ &&
      !baseline_enabled;
  const bool frontier_enabled = recovery_frontier_enabled_config_;

  // Production adaptive Succession reaches this function already validated and
  // with task_id parsed. Register directly under the manager's single planner
  // lock instead of re-entering the defensive public wrapper and rechecking the
  // same TaskSpec/configuration state.
  if (frontier_enabled && !returned_refs.empty()) {
    if (production_adaptive) {
      absl::MutexLock lock(&mutex_);
      static_cast<void>(
          RegisterOwnerTaskWithRecoveryFrontierLocked(task_spec, task_id));
    } else {
      static_cast<void>(RegisterOwnerTaskWithRecoveryFrontier(task_spec));
    }
  }

  // PERF-ONLY frontier-density owner-state selector.
  //
  // Match CoreWorker's TaskID selector so non-frontier tasks do not silently
  // retain baseline TaskManager/recovery state. This makes the experiment test
  // the cost of protecting only ~1/K tasks, not merely suppressing their
  // witness RPCs.
  if (baseline_enabled && !frontier_enabled) {
    const uint32_t protect_every_n =
        RayConfig::instance().recovery_baseline_perf_protect_every_n();
    if (protect_every_n > 1) {
      constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
      constexpr uint64_t kPrime = 1099511628211ULL;
      uint64_t task_hash = kOffsetBasis;
      const std::string task_id_binary = task_id.Binary();
      for (const unsigned char byte : task_id_binary) {
        task_hash ^= static_cast<uint64_t>(byte);
        task_hash *= kPrime;
      }
      if ((task_hash % protect_every_n) != 0) {
        return;
      }
    }
  }

  // Production adaptive Succession owns neither a duplicate TaskSpec nor
  // duplicate return-lifetime state here. TaskManager already owns both the
  // immutable recipe and reconstructable_return_ids_. Fixed-R/direct-manager
  // paths deliberately retain the old manager state for baseline isolation.
  if (production_adaptive) {
    return;
  }

  const bool task_manager_pin =
      baseline_enabled || recovery_succession_task_manager_pin_enabled_config_;

  // Legacy/direct-manager and Fixed-R paths use the exact ObjectID set.
  const bool succession_counter_lifetime = false;

  OwnerRetainedTaskState retained;
  uint64_t retained_copy_ns = 0;

  if (task_manager_pin) {
    // TaskManager already owns the TaskSpec. Keep only lifetime bookkeeping.
    // ByteSizeLong is profiling/accounting only; do not traverse a potentially
    // large TaskSpec on the production fast path when profiling is disabled.
    retained.task_spec_bytes =
        profiling_enabled_
            ? static_cast<uint64_t>(task_proto.ByteSizeLong())
            : 0;
  } else {
    const auto retained_copy_start = std::chrono::steady_clock::now();

    retained.task_spec.CopyFrom(task_proto);
    ClearFirstHolderTaskSpecPiggybacks(&retained.task_spec);
    retained.task_spec_bytes =
        static_cast<uint64_t>(retained.task_spec.ByteSizeLong());

    const auto retained_copy_end = std::chrono::steady_clock::now();
    retained_copy_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            retained_copy_end - retained_copy_start)
            .count());
  }

  for (const rpc::ObjectReference &returned_ref : returned_refs) {
    if (returned_ref.object_id().size() != ObjectID::Size()) {
      continue;
    }

    const ObjectID object_id = ObjectID::FromBinary(returned_ref.object_id());
    if (object_id.TaskId() != task_id) {
      continue;
    }

    if (succession_counter_lifetime) {
      ++retained.remaining_live_returns;
    } else {
      retained.live_return_ids.insert(object_id);
    }
  }

  if (!retained.HasLiveReturns()) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  auto existing = owner_retained_tasks_.find(task_id);
  if (existing == owner_retained_tasks_.end()) {
    if (profiling_enabled_) {
      ++profile_.owner_retained_task_specs_created;
      ++profile_.owner_retained_task_specs_current;
      profile_.owner_retained_task_spec_bytes_current +=
          retained.task_spec_bytes;
      profile_.owner_retained_task_spec_copy_time_ns += retained_copy_ns;

      if (task_manager_pin) {
        ++profile_.owner_lazy_task_spec_copies_avoided;
      }

      if (profile_.owner_retained_task_specs_current >
          profile_.owner_retained_task_specs_peak) {
        profile_.owner_retained_task_specs_peak =
            profile_.owner_retained_task_specs_current;
      }

      if (profile_.owner_retained_task_spec_bytes_current >
          profile_.owner_retained_task_spec_bytes_peak) {
        profile_.owner_retained_task_spec_bytes_peak =
            profile_.owner_retained_task_spec_bytes_current;
      }
    }

    owner_retained_tasks_[task_id] = std::move(retained);
    return;
  }

  if (succession_counter_lifetime) {
    // Static return registration is complete on the first CoreWorker call. A
    // repeated registration must not re-inflate the count after callbacks may
    // already have fired.
    return;
  }

  // Preserve the existing Fixed-R/legacy merge behavior exactly.
  for (const ObjectID &object_id : retained.live_return_ids) {
    existing->second.live_return_ids.insert(object_id);
  }
}

bool RecoverySuccessionManager::GetRetainedOwnerTaskSpec(
    const TaskID &task_id,
    rpc::TaskSpec *task_spec) const {
  if (task_spec == nullptr || task_id.IsNil()) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto it = owner_retained_tasks_.find(task_id);
  if (it == owner_retained_tasks_.end() ||
      !it->second.HasLiveReturns() ||
      it->second.task_spec.task_id().empty()) {
    // In 4N-PIN mode the dormant TaskSpec lives in TaskManager.
    return false;
  }

  task_spec->CopyFrom(it->second.task_spec);
  return true;
}

bool RecoverySuccessionManager::OwnerTaskHasLiveReturns(
    const TaskID &task_id) const {
  if (task_id.IsNil()) {
    return false;
  }

  absl::MutexLock lock(&mutex_);
  const auto it = owner_retained_tasks_.find(task_id);
  return it != owner_retained_tasks_.end() &&
         it->second.HasLiveReturns();
}

bool RecoverySuccessionManager::HandleOwnerTaskLineageReleased(
    const TaskID &task_id) {
  if (task_id.IsNil() ||
      recovery_witness_holder_baseline_enabled_config_) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  if (AdaptiveRecoveryFrontierEnabledCached()) {
    const auto membership = recovery_frontier_planner_->FindTask(task_id);
    if (membership.has_value()) {
      RecoveryFrontierGroup *group =
          recovery_frontier_planner_->GetMutableGroup(membership->group_id);
      RAY_CHECK(group != nullptr);

      if (!group->MarkOwnerTaskReleased(task_id)) {
        return false;
      }

      // The final live member released its native TaskManager lineage. Close a
      // partial group before a future task can append to a terminal capsule.
      RAY_CHECK(recovery_frontier_planner_->SealGroup(membership->group_id));

      if (recovery_frontier_protection_manifests_.contains(
              membership->group_id)) {
        return true;
      }

      // Never activated/exported: there is no remote state to tombstone.
      RAY_CHECK(recovery_frontier_planner_->EraseGroup(membership->group_id));
      return false;
    }
  }

  const auto task_it = task_states_.find(task_id);
  return task_it != task_states_.end() &&
         !task_it->second.manifest.tombstoned();
}

bool RecoverySuccessionManager::HandleOwnerReturnRefDeleted(
    const ObjectID &object_id,
    bool *final_return_deleted) {
  if (final_return_deleted != nullptr) {
    *final_return_deleted = false;
  }

  if (object_id.IsNil()) {
    return false;
  }

  const TaskID task_id = object_id.TaskId();

  absl::MutexLock lock(&mutex_);

  auto retained_it = owner_retained_tasks_.find(task_id);
  if (retained_it == owner_retained_tasks_.end()) {
    return false;
  }

  if (retained_it->second.remaining_live_returns > 0) {
    // Adaptive Succession registers exactly one deletion callback for each
    // static returned ObjectRef counted at owner completion.
    --retained_it->second.remaining_live_returns;
    if (retained_it->second.remaining_live_returns > 0) {
      return false;
    }
  } else {
    // Fixed-R and direct/legacy manager callers retain the original exact
    // ObjectID-set behavior.
    if (retained_it->second.live_return_ids.erase(object_id) == 0) {
      return false;
    }
    if (!retained_it->second.live_return_ids.empty()) {
      return false;
    }
  }

  if (final_return_deleted != nullptr) {
    *final_return_deleted = true;
  }

  const uint64_t retained_bytes = retained_it->second.task_spec_bytes;

  if (profiling_enabled_) {
    ++profile_.owner_retained_task_specs_released;

    if (profile_.owner_retained_task_specs_current > 0) {
      --profile_.owner_retained_task_specs_current;
    }

    if (profile_.owner_retained_task_spec_bytes_current >= retained_bytes) {
      profile_.owner_retained_task_spec_bytes_current -= retained_bytes;
    } else {
      profile_.owner_retained_task_spec_bytes_current = 0;
    }
  }

  owner_retained_tasks_.erase(retained_it);

  if (recovery_frontier_planner_ != nullptr &&
      recovery_frontier_planner_->GroupSize() > 1) {
    const auto membership = recovery_frontier_planner_->FindTask(task_id);
    if (membership.has_value()) {
      const RecoveryFrontierGroup *group =
          recovery_frontier_planner_->GetGroup(membership->group_id);
      RAY_CHECK(group != nullptr);

      for (const RecoveryFrontierMember &member : group->Members()) {
        const auto live_it = owner_retained_tasks_.find(member.task_id);
        if (live_it != owner_retained_tasks_.end() &&
            live_it->second.HasLiveReturns()) {
          return false;
        }
      }

      // This was the final live member. Close a partially filled group
      // before any future task can reuse its protection identity.
      RAY_CHECK(recovery_frontier_planner_->SealGroup(membership->group_id));

      if (recovery_frontier_protection_manifests_.contains(
              membership->group_id)) {
        // One shared tombstone will retire every committed member alias
        // on the fixed-R holders or adaptive Succession holders.
        return true;
      }

      // The group was never activated/exported, so there is no remote
      // recovery state to tombstone. Reclaim the owner-local planner state.
      RAY_CHECK(recovery_frontier_planner_->EraseGroup(membership->group_id));
      return false;
    }
  }

  const auto task_it = task_states_.find(task_id);
  return task_it != task_states_.end() &&
         !task_it->second.manifest.tombstoned();
}

std::vector<RecoverySuccessionManager::CandidateReport>
RecoverySuccessionManager::RegisterExecutorTask(const rpc::TaskSpec &task_spec) {
  const auto patch4g_start = std::chrono::steady_clock::now();

  // Compact Patch-4I sidecars are the common receiver path for adaptive
  // Succession. Keep pointers into the immutable incoming TaskSpec so ordinary
  // K=1 and Frontier both avoid materializing an intermediate synthetic
  // ObjectReference/RecoveryObjectMetadata protobuf before manager admission.
  struct DirectCompactDependency {
    ObjectID object_id;
    TaskID task_id;
    TaskID group_id;
    bool frontier_member = false;
    const rpc::RecoveryTaskArgumentMetadata *entry = nullptr;
  };

  std::vector<DirectCompactDependency> direct_compact_metadata;
  std::vector<std::pair<ObjectID, rpc::RecoveryObjectMetadata>> received_metadata;
  absl::flat_hash_set<ObjectID> received_object_ids;

  auto append_metadata = [&received_metadata, &received_object_ids](
                             const ObjectID &object_id,
                             rpc::RecoveryObjectMetadata metadata) {
    if (!received_object_ids.insert(object_id).second) {
      return;
    }
    received_metadata.emplace_back(object_id, std::move(metadata));
  };

  // Patch 4I primary path: one TaskSpec-level sidecar per unique dependency.
  // Compact adaptive-Succession entries stay compact here. Fixed-R, full or
  // legacy metadata, Patch-4F payloads, and malformed compact entries retain
  // the compatibility expansion below.
  for (const rpc::RecoveryTaskArgumentMetadata &entry :
       task_spec.recovery_argument_metadata()) {
    if (entry.object_id().size() != ObjectID::Size()) {
      continue;
    }

    const ObjectID object_id = ObjectID::FromBinary(entry.object_id());
    if (received_object_ids.contains(object_id)) {
      continue;
    }

    if (!recovery_witness_holder_baseline_enabled_config_ &&
        entry.has_owner_address() &&
        !entry.owner_address().worker_id().empty() &&
        entry.has_recovery_metadata() &&
        entry.recovery_metadata().has_compact_manifest()) {
      TaskID group_id;
      const rpc::RecoveryObjectMetadata &transport = entry.recovery_metadata();
      const rpc::RecoveryObjectTransportManifest &compact =
          transport.compact_manifest();
      bool valid_compact = compact.generation() != 0;
      bool frontier_member = false;

      // The only non-empty first-holder payload accepted on the direct path is
      // the typed Frontier membership marker. Any legacy/malformed payload
      // falls back to the compatibility path so Patch-4F semantics are unchanged.
      if (valid_compact && !transport.first_holder_task_spec().empty()) {
        frontier_member =
            AdaptiveRecoveryFrontierEnabledCached() &&
            recovery_succession_internal::ParseFrontierSuccessionMemberMarker(
                transport.first_holder_task_spec(), &group_id);
        if (!frontier_member) {
          valid_compact = false;
        }
      }

      if (valid_compact) {
        for (const rpc::Address &holder : compact.non_owner_holders()) {
          if (holder.worker_id().empty()) {
            valid_compact = false;
            break;
          }
        }
      }

      if (valid_compact && received_object_ids.insert(object_id).second) {
        DirectCompactDependency dependency;
        dependency.object_id = object_id;
        dependency.task_id = object_id.TaskId();
        dependency.group_id = group_id;
        dependency.frontier_member = frontier_member;
        dependency.entry = &entry;
        direct_compact_metadata.push_back(std::move(dependency));
        continue;
      }
    }

    rpc::RecoveryObjectMetadata metadata;
    if (!ExpandTaskSidecarRecoveryMetadata(entry, &metadata)) {
      continue;
    }
    append_metadata(object_id, std::move(metadata));
  }

  // Backward-compatible path for pre-4I TaskSpecs. A TaskSpec-level entry wins
  // if both representations are present for the same ObjectID.
  auto collect_legacy_metadata =
      [&received_object_ids, &append_metadata](const rpc::ObjectReference &object_ref) {
        if (object_ref.object_id().empty() || !object_ref.has_recovery_metadata() ||
            object_ref.object_id().size() != ObjectID::Size()) {
          return;
        }

        const ObjectID object_id = ObjectID::FromBinary(object_ref.object_id());
        if (received_object_ids.contains(object_id)) {
          return;
        }

        rpc::RecoveryObjectMetadata metadata;
        if (!ExpandTaskArgumentRecoveryMetadata(object_ref, &metadata)) {
          return;
        }

        append_metadata(object_id, std::move(metadata));
      };

  for (const rpc::TaskArg &arg : task_spec.args()) {
    if (arg.has_object_ref()) {
      collect_legacy_metadata(arg.object_ref());
    }

    for (const rpc::ObjectReference &nested_ref : arg.nested_inlined_refs()) {
      collect_legacy_metadata(nested_ref);
    }
  }

  const bool should_store_task = IsEligibleTask(task_spec) &&
                                 task_spec.has_recovery_manifest() &&
                                 !task_spec.task_id().empty();

  std::vector<CandidateReport> reports;
  absl::flat_hash_set<TaskID> piggyback_task_ids;
  absl::flat_hash_map<TaskID, TaskID> frontier_group_by_task;

  absl::MutexLock lock(&mutex_);

  // Direct compact receiver path. Construct exactly the task-local manifest
  // that compatibility expansion would produce, while avoiding:
  //   compact -> synthetic ObjectReference -> RecoveryObjectMetadata,
  // the received_metadata full-protobuf vector copy, and for ordinary K=1 the
  // later candidate-manifest copy as well. Frontier keeps its group-ID rewrite.
  for (const DirectCompactDependency &dependency : direct_compact_metadata) {
    RAY_CHECK(dependency.entry != nullptr);
    const rpc::RecoveryTaskArgumentMetadata &entry = *dependency.entry;
    const rpc::RecoveryObjectMetadata &transport = entry.recovery_metadata();
    const rpc::RecoveryObjectTransportManifest &compact =
        transport.compact_manifest();

    rpc::RecoveryManifest incoming_manifest;
    incoming_manifest.set_task_id(dependency.task_id.Binary());
    incoming_manifest.set_job_id(dependency.task_id.JobId().Binary());
    incoming_manifest.set_target_holder_count(compact.target_holder_count());
    incoming_manifest.set_witness_count(compact.witness_count());
    incoming_manifest.mutable_version()->set_generation(compact.generation());
    incoming_manifest.set_frozen(compact.frozen());
    incoming_manifest.set_tombstoned(compact.tombstoned());
    incoming_manifest.set_recovery_attempt(compact.recovery_attempt());
    incoming_manifest.set_max_recovery_attempts(compact.max_recovery_attempts());

    for (const rpc::Address &witness : compact.witness_raylets()) {
      incoming_manifest.add_witness_raylets()->CopyFrom(witness);
    }

    rpc::RecoveryHolder *owner = incoming_manifest.add_succession();
    owner->mutable_address()->CopyFrom(entry.owner_address());
    owner->set_rank(0);
    owner->set_failure_domain_id(entry.owner_address().node_id());

    for (int i = 0; i < compact.non_owner_holders_size(); ++i) {
      const rpc::Address &address = compact.non_owner_holders(i);
      rpc::RecoveryHolder *holder = incoming_manifest.add_succession();
      holder->mutable_address()->CopyFrom(address);
      holder->set_rank(static_cast<uint32_t>(i + 1));
      holder->set_failure_domain_id(address.node_id());
    }

    const rpc::RecoveryManifest *effective_manifest = &incoming_manifest;
    uint32_t effective_return_index = transport.return_index();
    const auto existing_metadata =
        object_recovery_metadata_.find(dependency.object_id);
    if (existing_metadata != object_recovery_metadata_.end() &&
        existing_metadata->second.has_manifest() &&
        CompareManifestVersions(existing_metadata->second.manifest(),
                                incoming_manifest) > 0) {
      effective_manifest = &existing_metadata->second.manifest();
      effective_return_index = existing_metadata->second.return_index();
    }

    const auto tombstone_it = task_states_.find(dependency.task_id);
    if (tombstone_it != task_states_.end() &&
        tombstone_it->second.manifest.tombstoned() &&
        CompareManifestVersions(tombstone_it->second.manifest,
                                *effective_manifest) >= 0) {
      continue;
    }

    BorrowedObjectRecoveryState borrowed_state;
    borrowed_state.task_id = dependency.task_id;
    borrowed_state.return_index = effective_return_index;
    borrowed_objects_[dependency.object_id] = std::move(borrowed_state);

    TaskRecoveryState &dependency_state = task_states_[dependency.task_id];
    if (dependency_state.manifest.task_id().empty() ||
        CompareManifestVersions(*effective_manifest,
                                dependency_state.manifest) > 0) {
      dependency_state.manifest.CopyFrom(*effective_manifest);
    }

    if (dependency.frontier_member) {
      if (dependency.task_id == dependency.group_id &&
          !entry.initial_frontier_recipe().empty()) {
        StoreInitialFrontierPiggybackLocked(
            entry.initial_frontier_recipe(), incoming_manifest,
            task_spec.caller_address());
      }
      // Candidate admission is group-centric. Once this borrower has reported
      // the group, later members skip candidate-manifest construction entirely.
      if (!candidate_reports_sent_.contains(dependency.group_id)) {
        rpc::RecoveryManifest candidate_manifest;
        candidate_manifest.CopyFrom(incoming_manifest);
        candidate_manifest.set_task_id(dependency.group_id.Binary());
        MaybeAddCandidateReportLocked(
            candidate_manifest,
            /*already_stores_task_spec=*/false,
            &reports);
      }
    } else {
      // Ordinary K=1 Succession preserves the exact task-local admission
      // semantics but no longer deep-copies the compact manifest through the
      // compatibility vector and a second candidate_manifest temporary.
      MaybeAddCandidateReportLocked(
          incoming_manifest,
          /*already_stores_task_spec=*/false,
          &reports);
    }
  }

  // Compatibility path for Fixed-R, legacy/full transport, Patch-4F payloads,
  // and any compact sidecar that could not use the direct adaptive path.
  for (const auto &[object_id, metadata] : received_metadata) {
    TaskID frontier_group_id;
    const bool frontier_member =
        AdaptiveRecoveryFrontierEnabledCached() &&
        recovery_succession_internal::ParseFrontierSuccessionMemberMarker(
            metadata.first_holder_task_spec(), &frontier_group_id);

    // Parse the Patch-4F transport sidecar before normal metadata selection.
    // A Frontier membership marker occupies the same transport-only field but
    // is not a serialized TaskSpec and must not be treated as one.
    rpc::TaskSpec piggyback_task_spec;
    bool valid_piggyback = false;

    if (!metadata.first_holder_task_spec().empty() && !frontier_member) {
      valid_piggyback =
          piggyback_task_spec.ParseFromString(metadata.first_holder_task_spec()) &&
          !piggyback_task_spec.task_id().empty() &&
          piggyback_task_spec.task_id() == metadata.task_id() &&
          IsEligibleTask(piggyback_task_spec);

      if (valid_piggyback) {
        ClearFirstHolderTaskSpecPiggybacks(&piggyback_task_spec);
      } else {
        RAY_LOG(DEBUG)
            << "Ignoring invalid Patch 4F first-holder TaskSpec piggyback";
      }
    }

    rpc::RecoveryObjectMetadata effective_metadata;
    effective_metadata.CopyFrom(metadata);

    const auto existing_metadata = object_recovery_metadata_.find(object_id);

    if (existing_metadata != object_recovery_metadata_.end() &&
        CompareManifestVersions(existing_metadata->second.manifest(),
                                metadata.manifest()) > 0) {
      effective_metadata.CopyFrom(existing_metadata->second);
    }

    // Transport only: do not recursively forward full TaskSpecs or group markers.
    effective_metadata.clear_first_holder_task_spec();

    const TaskID metadata_task_id =
        TaskID::FromBinary(effective_metadata.task_id());

    if (frontier_member) {
      frontier_group_by_task[metadata_task_id] = frontier_group_id;
    }

    const auto tombstone_it = task_states_.find(metadata_task_id);
    if (tombstone_it != task_states_.end() &&
        tombstone_it->second.manifest.tombstoned() &&
        CompareManifestVersions(tombstone_it->second.manifest,
                                effective_metadata.manifest()) >= 0) {
      continue;
    }

    BorrowedObjectRecoveryState borrowed_state;
    borrowed_state.task_id = metadata_task_id;
    borrowed_state.return_index = effective_metadata.return_index();
    borrowed_objects_[object_id] = std::move(borrowed_state);

    TaskRecoveryState &dependency_state = task_states_[metadata_task_id];
    if (dependency_state.manifest.task_id().empty() ||
        CompareManifestVersions(effective_metadata.manifest(),
                                dependency_state.manifest) > 0) {
      dependency_state.manifest.CopyFrom(effective_metadata.manifest());
    }

    if (valid_piggyback &&
        piggyback_task_spec.task_id() == effective_metadata.task_id()) {
      if (!dependency_state.task_spec.has_value()) {
        piggyback_task_spec.mutable_recovery_manifest()->CopyFrom(
            dependency_state.manifest);
        dependency_state.task_spec = std::move(piggyback_task_spec);
        dependency_state.manifest_committed = false;
        dependency_state.provisional_piggyback_task_spec = true;
        piggyback_task_ids.insert(metadata_task_id);
      } else if (dependency_state.provisional_piggyback_task_spec) {
        piggyback_task_ids.insert(metadata_task_id);
      }
    }
  }

  if (should_store_task) {
    const TaskID task_id = TaskID::FromBinary(task_spec.task_id());

    const auto existing_task_it = task_states_.find(task_id);

    const bool stale_after_tombstone =
        existing_task_it != task_states_.end() &&
        existing_task_it->second.manifest.tombstoned() &&
        CompareManifestVersions(existing_task_it->second.manifest,
                                task_spec.recovery_manifest()) >= 0;

    if (!stale_after_tombstone) {
      TaskRecoveryState &task_state = task_states_[task_id];

      if (task_state.manifest.task_id().empty() ||
          CompareManifestVersions(task_spec.recovery_manifest(),
                                  task_state.manifest) > 0) {
        task_state.manifest.CopyFrom(task_spec.recovery_manifest());
      }

      rpc::TaskSpec stored_task_spec;
      stored_task_spec.CopyFrom(task_spec);
      ClearFirstHolderTaskSpecPiggybacks(&stored_task_spec);
      stored_task_spec.mutable_recovery_manifest()->CopyFrom(task_state.manifest);

      task_state.task_spec = std::move(stored_task_spec);

      // IMPORTANT: the original executor is not admitted as a durable recovery
      // holder. Holder admission still comes only from downstream borrowers.
    }
  }

  for (const auto &[object_id, metadata] : received_metadata) {
    static_cast<void>(object_id);

    const TaskID metadata_task_id = TaskID::FromBinary(metadata.task_id());
    rpc::RecoveryManifest candidate_manifest;
    candidate_manifest.CopyFrom(metadata.manifest());
    bool already_stores_task_spec =
        piggyback_task_ids.contains(metadata_task_id);

    const auto group_it = frontier_group_by_task.find(metadata_task_id);
    if (group_it != frontier_group_by_task.end()) {
      // Candidate admission is group-centric: all members of this Frontier
      // share one adaptive Succession chain. Replay remains task-centric.
      candidate_manifest.set_task_id(group_it->second.Binary());
      already_stores_task_spec = false;
    }

    MaybeAddCandidateReportLocked(
        candidate_manifest,
        already_stores_task_spec,
        &reports);
  }

  // Nested dependency order is not group order: a non-leader can create the
  // candidate report before the leader's recipe sidecar is processed. Derive
  // stored-prefix proofs only after this TaskSpec's metadata has all been
  // consumed. Reports are still local here and have not reached CoreWorker's
  // RPC queue. This adds no event-loop turn or wait for another export.
  if (InitialFrontierPiggybackEnabledCached()) {
    for (auto &report : reports) {
      const TaskID task_id = TaskID::FromBinary(report.request.task_id());
      const auto *group = recovery_frontier_planner_->GetGroup(task_id);
      if (group != nullptr && group->Full() &&
          group->MemberCount() == recovery_frontier_group_size_config_ &&
          group->CommittedMemberCount() == group->MemberCount() &&
          group->Generation() == 1) {
        bool retained = true;
        for (const auto &member : group->Members()) {
          const auto stored = task_states_.find(member.task_id);
          if (stored == task_states_.end() ||
              !stored->second.task_spec.has_value() ||
              stored->second.task_spec->task_id() != member.task_id.Binary() ||
              stored->second.manifest.tombstoned() ||
              !stored->second.provisional_piggyback_task_spec ||
              stored->second.manifest_committed) {
            retained = false;
            break;
          }
        }
        if (retained) {
          report.request.set_already_stores_task_spec(true);
          report.request.set_stored_frontier_generation(group->Generation());
          for (const auto &member : group->Members()) {
            report.request.add_stored_frontier_member_ids(member.task_id.Binary());
          }
        }
      }
    }
  }

  if (profiling_enabled_) {
    ++profile_.register_executor_task_calls;
    profile_.register_executor_task_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
    profile_.register_executor_metadata_refs_seen +=
        static_cast<uint64_t>(direct_compact_metadata.size() +
                              received_metadata.size());
    profile_.register_executor_candidate_reports_built +=
        static_cast<uint64_t>(reports.size());
  }

  return reports;
}


void RecoverySuccessionManager::RegisterBorrowedObject(
    const ObjectID &object_id, const rpc::RecoveryObjectMetadata &metadata) {
  if (metadata.task_id().empty() || !metadata.has_manifest()) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(metadata.task_id());

  absl::MutexLock lock(&mutex_);

  const auto tombstone_it = task_states_.find(task_id);
  if (tombstone_it != task_states_.end() &&
      tombstone_it->second.manifest.tombstoned() &&
      CompareManifestVersions(tombstone_it->second.manifest,
                              metadata.manifest()) >= 0) {
    return;
  }

  BorrowedObjectRecoveryState borrowed_state;
  borrowed_state.task_id = task_id;
  borrowed_state.return_index = metadata.return_index();
  borrowed_objects_[object_id] = std::move(borrowed_state);

  TaskRecoveryState &task_state = task_states_[task_id];
  if (task_state.manifest.task_id().empty() ||
      CompareManifestVersions(metadata.manifest(), task_state.manifest) > 0) {
    task_state.manifest.CopyFrom(metadata.manifest());
  }
}

rpc::ReportRecoveryCandidateReply::Result
RecoverySuccessionManager::PrepareHolderAdmission(
    const rpc::ReportRecoveryCandidateRequest &request,
    const rpc::TaskSpec *owner_task_spec,
    HolderAdmissionPlan *plan,
    rpc::RecoveryManifest *latest_manifest) {
  if (plan == nullptr || latest_manifest == nullptr || request.task_id().empty() ||
      !request.has_candidate_address() || !request.has_cached_manifest()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  *plan = HolderAdmissionPlan();
  latest_manifest->Clear();

  const rpc::Address &candidate_address = request.candidate_address();

  if (candidate_address.worker_id().empty() || candidate_address.node_id().empty() ||
      candidate_address.ip_address().empty() || candidate_address.port() <= 0) {
    return rpc::ReportRecoveryCandidateReply::NO_SLOT;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());

  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);
  if (task_it == task_states_.end()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  const bool frontier_group_admission =
      AdaptiveRecoveryFrontierEnabledCached() &&
      recovery_frontier_planner_->GetGroup(task_id) != nullptr;

  std::optional<RecoveryFrontierAppendBatch> frontier_install_batch;
  if (frontier_group_admission) {
    RecoveryFrontierGroup *group =
        recovery_frontier_planner_->GetMutableGroup(task_id);
    RAY_CHECK(group != nullptr);

    // Freeze only the INITIAL RECIPE PREFIX, not the Frontier itself. This
    // guarantees that every concurrently admitted H1..HR receives the exact
    // same snapshot while later owner tasks remain free to join the group.
    if (group->CommittedMemberCount() == 0) {
      auto initial_it =
          adaptive_frontier_initial_append_batches_.find(task_id);
      if (initial_it ==
          adaptive_frontier_initial_append_batches_.end()) {
        auto staged = group->StageAppend();
        if (!staged.has_value()) {
          return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
        }
        initial_it =
            adaptive_frontier_initial_append_batches_
                .emplace(task_id, std::move(staged.value()))
                .first;
      }
      frontier_install_batch = initial_it->second;
    }
  }

  const TaskRecoveryState &task_state = task_it->second;
  latest_manifest->CopyFrom(task_state.manifest);

  if (task_state.manifest.tombstoned()) {
    return rpc::ReportRecoveryCandidateReply::TOMBSTONED;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(task_state.manifest, 0);
  if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
    return rpc::ReportRecoveryCandidateReply::WRONG_COORDINATOR;
  }

  if (request.cached_manifest().version().generation() >
      task_state.manifest.version().generation()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  if (ContainsWorker(task_state.manifest, candidate_address)) {
    return rpc::ReportRecoveryCandidateReply::ACCEPTED;
  }

  if (task_state.manifest.frozen()) {
    return rpc::ReportRecoveryCandidateReply::FROZEN;
  }

  const uint32_t confirmed_non_owner_holders =
      task_state.manifest.succession_size() > 0
          ? static_cast<uint32_t>(task_state.manifest.succession_size() - 1)
          : 0;

  const auto per_task_it = holder_reservation_by_task_.find(task_id);
  const size_t pending_count =
      per_task_it == holder_reservation_by_task_.end()
          ? 0
          : per_task_it->second.size();

  if (confirmed_non_owner_holders + pending_count >=
      task_state.manifest.target_holder_count()) {
    return rpc::ReportRecoveryCandidateReply::NO_SLOT;
  }

  // Patch 4D: a task may have several provisional reservations. Reject only
  // duplicate/failure-domain candidates; do not reject merely because another
  // rank is currently being installed.
  for (const rpc::RecoveryHolder &holder : task_state.manifest.succession()) {
    if (!holder.failure_domain_id().empty() &&
        holder.failure_domain_id() == candidate_address.node_id()) {
      return rpc::ReportRecoveryCandidateReply::NO_SLOT;
    }
  }

  if (per_task_it != holder_reservation_by_task_.end()) {
    for (const auto &[rank, existing_reservation_id] : per_task_it->second) {
      static_cast<void>(rank);
      const auto reservation_it = holder_reservations_.find(existing_reservation_id);
      if (reservation_it == holder_reservations_.end()) {
        continue;
      }

      const HolderReservation &pending = reservation_it->second;
      if (SameWorker(pending.candidate_address, candidate_address)) {
        // The original report RPC for this candidate is still responsible for
        // completing the admission. Treat duplicate reports as already accepted.
        return rpc::ReportRecoveryCandidateReply::ACCEPTED;
      }

      if (!pending.candidate_address.node_id().empty() &&
          pending.candidate_address.node_id() == candidate_address.node_id()) {
        return rpc::ReportRecoveryCandidateReply::NO_SLOT;
      }
    }
  }

  // Construct the speculative prefix from the committed manifest plus all
  // earlier reservations. Every proposed manifest is therefore contiguous:
  // [A,H1], [A,H1,H2], ... even while H1..HR installations overlap.
  rpc::RecoveryManifest proposed_manifest;
  proposed_manifest.CopyFrom(task_state.manifest);

  if (per_task_it != holder_reservation_by_task_.end()) {
    for (const auto &[rank, existing_reservation_id] : per_task_it->second) {
      const auto reservation_it = holder_reservations_.find(existing_reservation_id);
      if (reservation_it == holder_reservations_.end()) {
        continue;
      }

      const HolderReservation &pending = reservation_it->second;
      rpc::RecoveryHolder *holder = proposed_manifest.add_succession();
      holder->mutable_address()->CopyFrom(pending.candidate_address);
      holder->set_rank(rank);
      holder->set_failure_domain_id(pending.candidate_address.node_id());
    }
  }

  const uint32_t proposed_rank =
      static_cast<uint32_t>(proposed_manifest.succession_size());

  rpc::RecoveryHolder *new_holder = proposed_manifest.add_succession();
  new_holder->mutable_address()->CopyFrom(candidate_address);
  new_holder->set_rank(proposed_rank);
  new_holder->set_failure_domain_id(candidate_address.node_id());

  proposed_manifest.mutable_version()->set_generation(
      task_state.manifest.version().generation() + pending_count + 1);

  const uint32_t holders_after_admission =
      static_cast<uint32_t>(proposed_manifest.succession_size() - 1);
  if (holders_after_admission >= proposed_manifest.target_holder_count()) {
    proposed_manifest.set_frozen(true);
  }

  const std::string reservation_id = UniqueID::FromRandom().Binary();

  HolderReservation reservation;
  reservation.task_id = task_id;
  reservation.candidate_address.CopyFrom(candidate_address);
  reservation.proposed_manifest.CopyFrom(proposed_manifest);
  reservation.proposed_rank = proposed_rank;

  holder_reservations_[reservation_id] = std::move(reservation);
  holder_reservation_by_task_[task_id][proposed_rank] = reservation_id;

  plan->reservation_id = reservation_id;
  plan->candidate_address.CopyFrom(candidate_address);
  plan->candidate_already_stores_task_spec =
      frontier_group_admission ? false : request.already_stores_task_spec();
  if (frontier_group_admission && InitialFrontierPiggybackEnabledCached() &&
      request.already_stores_task_spec() && frontier_install_batch.has_value()) {
    const auto &batch = frontier_install_batch.value();
    const uint32_t member_count = recovery_frontier_group_size_config_;
    bool matches = batch.begin_member_index == 0 &&
                   batch.end_member_index == member_count &&
                   batch.members.size() == member_count &&
                   request.stored_frontier_generation() == batch.generation &&
                   static_cast<uint32_t>(request.stored_frontier_member_ids_size()) ==
                       member_count;
    for (int i = 0; matches && i < request.stored_frontier_member_ids_size(); ++i) {
      matches = request.stored_frontier_member_ids(i) ==
                batch.members[i].task_id.Binary();
    }
    plan->candidate_already_stores_task_spec = matches;
    if (matches && profiling_enabled_) {
      ++profile_.frontier_recipe_piggyback_admissions;
    }
  }

  if (!plan->candidate_already_stores_task_spec) {
    const rpc::TaskSpec *lineage_task_spec = nullptr;

    if (task_it->second.task_spec.has_value()) {
      lineage_task_spec = &task_it->second.task_spec.value();
    } else if (owner_task_spec != nullptr) {
      lineage_task_spec = owner_task_spec;
    } else if (frontier_group_admission && InitialFrontierPiggybackEnabledCached()) {
      // A missing/mismatched storage proof takes the ordinary install path.
      // The planner still owns the leader recipe even when CoreWorker skipped
      // a TaskManager lookup because the request claimed local storage.
      const auto *group = recovery_frontier_planner_->GetGroup(task_id);
      if (group != nullptr && !group->Members().empty()) {
        lineage_task_spec = group->Members().front().task_spec.get();
      }
    } else {
      // Patch 4L: TaskManager may have legitimately dropped ordinary lineage
      // while the application still owns a return ObjectRef.
      const auto retained_it = owner_retained_tasks_.find(task_id);
      if (retained_it != owner_retained_tasks_.end() &&
          retained_it->second.HasLiveReturns() &&
          !retained_it->second.task_spec.task_id().empty()) {
        lineage_task_spec = &retained_it->second.task_spec;
      }
    }

    if (lineage_task_spec == nullptr ||
        lineage_task_spec->task_id() != task_id.Binary() ||
        !IsEligibleTask(*lineage_task_spec)) {
      EraseHolderReservationLocked(reservation_id);
      return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
    }

    if (profiling_enabled_) {
      const auto copy_start = std::chrono::steady_clock::now();

      plan->task_spec.CopyFrom(*lineage_task_spec);
      ClearFirstHolderTaskSpecPiggybacks(&plan->task_spec);
      plan->task_spec.mutable_recovery_manifest()->CopyFrom(proposed_manifest);

      const auto copy_end = std::chrono::steady_clock::now();
      const uint64_t copy_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(copy_end - copy_start)
              .count());

      ++profile_.owner_task_spec_copy_count;
      profile_.owner_task_spec_copy_time_ns += copy_ns;
    } else {
      plan->task_spec.CopyFrom(*lineage_task_spec);
      ClearFirstHolderTaskSpecPiggybacks(&plan->task_spec);
      plan->task_spec.mutable_recovery_manifest()->CopyFrom(proposed_manifest);
    }
  }

  if (frontier_group_admission && !plan->candidate_already_stores_task_spec) {
    const RecoveryFrontierGroup *group =
        recovery_frontier_planner_->GetGroup(task_id);
    RAY_CHECK(group != nullptr);
    const auto encode_start =
        profiling_enabled_ ? std::chrono::steady_clock::now()
                           : std::chrono::steady_clock::time_point{};
    rpc::RecoveryFrontierAppend snapshot;
    const bool built =
        frontier_install_batch.has_value()
            ? recovery_succession_internal::BuildFrontierSuccessionAppend(
                  frontier_install_batch.value(), &snapshot)
            : recovery_succession_internal::BuildFrontierSuccessionSnapshot(
                  *group, &snapshot);
    if (!built) {
      EraseHolderReservationLocked(reservation_id);
      return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
    }
    recovery_succession_internal::PutFrontierSuccessionAppendCapsule(
        snapshot, &plan->task_spec);
    if (profiling_enabled_) {
      // mutex_ is already held; do not call the locking recorder here.
      ++profile_.frontier_recipe_encode_calls;
      profile_.frontier_recipe_encode_time_ns +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - encode_start).count();
      profile_.frontier_recipe_encode_members += snapshot.members_size();
      profile_.frontier_recipe_encode_bytes +=
          plan->task_spec.recovery_argument_metadata(0)
              .recovery_metadata().first_holder_task_spec().size();
    }
  }

  plan->proposed_manifest.CopyFrom(proposed_manifest);
  return rpc::ReportRecoveryCandidateReply::ACCEPTED;
}

bool RecoverySuccessionManager::StoreInitialFrontierPiggybackLocked(
    const std::string &serialized_snapshot,
    const rpc::RecoveryManifest &manifest,
    const rpc::Address &sender) {
  if (!InitialFrontierPiggybackEnabledCached()) {
    return false;
  }
  const auto store_start =
      profiling_enabled_ ? std::chrono::steady_clock::now()
                         : std::chrono::steady_clock::time_point{};
  rpc::RecoveryFrontierAppend snapshot;
  if (!snapshot.ParseFromString(serialized_snapshot)) {
    return false;
  }
  const uint32_t member_count = recovery_frontier_group_size_config_;
  if (snapshot.group_id().size() != TaskID::Size() ||
      snapshot.group_id() != manifest.task_id() ||
      snapshot.base_generation() != 0 || snapshot.generation() != 1 ||
      snapshot.begin_member_index() != 0 ||
      snapshot.end_member_index() != member_count ||
      static_cast<uint32_t>(snapshot.members_size()) != member_count ||
      snapshot.members(0).task_id() != snapshot.group_id() ||
      manifest.tombstoned() || manifest.frozen() ||
      manifest.target_holder_count() != recovery_succession_target_holder_count_config_ ||
      manifest.witness_count() != recovery_succession_witness_count_config_ ||
      static_cast<uint32_t>(manifest.witness_raylets_size()) != manifest.witness_count() ||
      ContainsWorker(manifest, self_address_)) {
    return false;
  }
  const auto *owner = FindHolderByRank(manifest, 0);
  if (owner == nullptr || !SameWorker(owner->address(), sender) ||
      SameWorker(owner->address(), self_address_) ||
      owner->address().node_id() == self_address_.node_id()) {
    return false;
  }

  const TaskID group_id = TaskID::FromBinary(snapshot.group_id());
  bool already_provisional = true;
  // Validate every member before installing anything. Never replace a
  // tombstone, a newer view, or an RPC-installed/confirmed holder with a
  // provisional payload from an older in-flight PushTask.
  for (const auto &record : snapshot.members()) {
    if (record.task_id().size() != TaskID::Size() ||
        !record.has_task_spec() ||
        record.task_spec().task_id() != record.task_id() ||
        !IsEligibleTask(record.task_spec()) ||
        !SameWorker(record.task_spec().caller_address(), owner->address())) {
      return false;
    }
    const auto existing = task_states_.find(TaskID::FromBinary(record.task_id()));
    if (existing == task_states_.end()) {
      already_provisional = false;
      continue;
    }
    const auto &state = existing->second;
    if (state.manifest.tombstoned() ||
        CompareManifestVersions(state.manifest, manifest) > 0 ||
        (state.task_spec.has_value() &&
         (state.manifest_committed || !state.provisional_piggyback_task_spec ||
          !state.provisional_reservation_id.empty()))) {
      return false;
    }
    already_provisional = already_provisional && state.task_spec.has_value() &&
                          state.provisional_piggyback_task_spec;
  }

  // This is the same recipe-import primitive used by InstallRecoveryHolder.
  // Its planner prefix means locally stored recipes, not holder durability:
  // every task state below remains provisional and uses the witness check.
  if (!recovery_frontier_planner_->ApplyCommittedAppend(snapshot)) {
    return false;
  }
  if (already_provisional) {
    return true;
  }
  const auto *group = recovery_frontier_planner_->GetGroup(group_id);
  RAY_CHECK(group != nullptr && group->MemberCount() == member_count);
  recovery_frontier_protection_manifests_[group_id].CopyFrom(manifest);

  const auto materialize_start =
      profiling_enabled_ ? std::chrono::steady_clock::now()
                         : std::chrono::steady_clock::time_point{};
  for (const auto &member : group->Members()) {
    auto member_manifest =
        recovery_succession_internal::BuildFrontierMemberManifest(manifest, member);
    auto &state = task_states_[member.task_id];
    state.manifest.CopyFrom(member_manifest);
    StoreFrontierHolderRecipeLocked(member.task_spec, member_manifest, &state);
    state.manifest_committed = false;
    state.provisional_reservation_id.clear();
    state.provisional_piggyback_task_spec = true;
  }
  if (profiling_enabled_) {
    ++profile_.frontier_holder_materialize_calls;
    profile_.frontier_holder_materialize_members += member_count;
    profile_.frontier_holder_materialize_time_ns +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - materialize_start).count();
    ++profile_.frontier_recipe_piggybacks_stored;
    profile_.frontier_recipe_piggyback_store_time_ns +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - store_start).count();
  }
  return true;
}

bool RecoverySuccessionManager::InstallRecoveryHolder(
    const rpc::InstallRecoveryHolderRequest &request) {
  const auto handler_start =
      profiling_enabled_ ? std::chrono::steady_clock::now()
                         : std::chrono::steady_clock::time_point{};
  // Declared before mutex_'s guard so recording happens after it is released.
  absl::Cleanup handler_profile = [this, handler_start]() {
    if (profiling_enabled_) {
      RecordHolderInstallHandler(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - handler_start).count());
    }
  };
  if (request.task_id().empty() || request.reservation_id().empty() ||
      !request.has_task_spec() || !request.has_proposed_manifest() ||
      request.task_spec().task_id() != request.task_id() ||
      request.proposed_manifest().task_id() != request.task_id() ||
      !IsEligibleTask(request.task_spec())) {
    return false;
  }

  const rpc::RecoveryHolder *proposed_holder =
      FindHolderByRank(request.proposed_manifest(), request.proposed_rank());

  if (proposed_holder == nullptr ||
      !SameWorker(proposed_holder->address(), self_address_) ||
      proposed_holder->failure_domain_id() != self_address_.node_id()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());
  rpc::RecoveryFrontierAppend frontier_snapshot;
  const bool carries_frontier_snapshot =
      recovery_succession_internal::ExtractFrontierSuccessionAppendCapsule(
          request.task_spec(), &frontier_snapshot);

  absl::MutexLock lock(&mutex_);

  if (carries_frontier_snapshot && AdaptiveRecoveryFrontierEnabledCached()) {
    if (InitialFrontierPiggybackEnabledCached()) {
      const auto terminal = task_states_.find(task_id);
      if (terminal != task_states_.end() && terminal->second.manifest.tombstoned()) {
        return false;
      }
    }
    if (frontier_snapshot.group_id() != request.task_id() ||
        recovery_frontier_planner_ == nullptr ||
        !recovery_frontier_planner_->ApplyCommittedAppend(frontier_snapshot)) {
      return false;
    }

    const RecoveryFrontierGroup *group =
        recovery_frontier_planner_->GetGroup(task_id);
    if (group == nullptr || group->MemberCount() == 0) {
      return false;
    }

    const auto materialize_start =
        profiling_enabled_ ? std::chrono::steady_clock::now()
                           : std::chrono::steady_clock::time_point{};
    for (const RecoveryFrontierMember &member : group->Members()) {
      if (member.task_spec == nullptr || !IsEligibleTask(*member.task_spec)) {
        return false;
      }

      rpc::RecoveryManifest member_manifest =
          recovery_succession_internal::BuildFrontierMemberManifest(
              request.proposed_manifest(), member);
      TaskRecoveryState &member_state = task_states_[member.task_id];

      if (!member_state.manifest.task_id().empty() &&
          CompareManifestVersions(member_state.manifest, member_manifest) > 0) {
        return false;
      }

      member_state.manifest.CopyFrom(member_manifest);
      StoreFrontierHolderRecipeLocked(member.task_spec, member_manifest, &member_state);
      member_state.manifest_committed = false;
      member_state.provisional_reservation_id = request.reservation_id();
      member_state.provisional_piggyback_task_spec = false;
    }

    if (profiling_enabled_) {
      ++profile_.frontier_holder_materialize_calls;
      profile_.frontier_holder_materialize_time_ns +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - materialize_start).count();
      profile_.frontier_holder_materialize_members += group->MemberCount();
    }
    candidate_reports_sent_.insert(task_id);
    return true;
  }

  TaskRecoveryState &task_state = task_states_[task_id];

  if (!task_state.manifest.task_id().empty()) {
    const int comparison =
        CompareManifestVersions(task_state.manifest, request.proposed_manifest());

    if (comparison > 0) {
      return false;
    }

    if (comparison == 0 && task_state.manifest_committed &&
        ContainsWorker(task_state.manifest, self_address_)) {
      return true;
    }
  }

  task_state.manifest.CopyFrom(request.proposed_manifest());

  rpc::TaskSpec stored_task_spec;
  stored_task_spec.CopyFrom(request.task_spec());
  ClearFirstHolderTaskSpecPiggybacks(&stored_task_spec);
  stored_task_spec.mutable_recovery_manifest()->CopyFrom(request.proposed_manifest());

  task_state.task_spec = std::move(stored_task_spec);
  task_state.manifest_committed = false;
  task_state.provisional_reservation_id = request.reservation_id();
  task_state.provisional_piggyback_task_spec = false;

  candidate_reports_sent_.insert(task_id);

  return true;
}

bool RecoverySuccessionManager::CommitHolderAdmission(
    const std::string &reservation_id, rpc::RecoveryManifest *committed_manifest) {
  if (reservation_id.empty() || committed_manifest == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return false;
  }

  const HolderReservation &reservation = reservation_it->second;
  const TaskID task_id = reservation.task_id;
  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    EraseHolderReservationLocked(reservation_id);
    return false;
  }

  const rpc::RecoveryManifest &current = task_it->second.manifest;
  const rpc::RecoveryManifest &proposed = reservation.proposed_manifest;

  auto committed_frontier_member_limit =
      [this, &task_id](const RecoveryFrontierGroup &group) {
        uint32_t limit = group.CommittedMemberCount();
        const auto initial_it =
            adaptive_frontier_initial_append_batches_.find(task_id);
        if (initial_it != adaptive_frontier_initial_append_batches_.end()) {
          limit = std::max(
              limit, initial_it->second.end_member_index);
        }
        return limit;
      };

  auto update_committed_topology =
      [this, &task_id, &committed_frontier_member_limit](
          const rpc::RecoveryManifest &group_manifest) {
        if (!AdaptiveRecoveryFrontierEnabledCached()) {
          UpdateManifestForTaskLocked(task_id, group_manifest, true);
          return;
        }

        const RecoveryFrontierGroup *group =
            recovery_frontier_planner_->GetGroup(task_id);
        if (group == nullptr) {
          UpdateManifestForTaskLocked(task_id, group_manifest, true);
          return;
        }

        recovery_frontier_protection_manifests_[task_id].CopyFrom(group_manifest);
        const uint32_t member_limit =
            committed_frontier_member_limit(*group);
        for (const RecoveryFrontierMember &member : group->Members()) {
          if (member.member_index >= member_limit) {
            break;
          }
          rpc::RecoveryManifest member_manifest =
              recovery_succession_internal::BuildFrontierMemberManifest(
                  group_manifest, member);
          UpdateManifestForTaskLocked(member.task_id, member_manifest, true);
        }
      };

  auto commit_initial_frontier_prefix_if_ready =
      [this, &task_id](const rpc::RecoveryManifest &group_manifest) {
        if (!AdaptiveRecoveryFrontierEnabledCached() ||
            !group_manifest.frozen()) {
          return true;
        }

        const auto initial_it =
            adaptive_frontier_initial_append_batches_.find(task_id);
        if (initial_it == adaptive_frontier_initial_append_batches_.end()) {
          return true;
        }

        RecoveryFrontierGroup *group =
            recovery_frontier_planner_->GetMutableGroup(task_id);
        if (group == nullptr ||
            !group->CommitAppend(initial_it->second)) {
          return false;
        }
        adaptive_frontier_initial_append_batches_.erase(initial_it);
        return true;
      };


  // Patch 4M-CERT independent commit.  Witness ACK authorizes exactly this
  // reservation's candidate; it does not require lower admission slots to have
  // committed first.  Materialized ranks are derived deterministically.
  if (recovery_succession_certificate_admission_enabled_config_ &&
      !recovery_witness_holder_baseline_enabled_config_) {
    const rpc::RecoveryHolder *candidate =
        FindHolderByRank(proposed, reservation.proposed_rank);
    if (candidate == nullptr ||
        !SameWorker(candidate->address(), reservation.candidate_address)) {
      return false;
    }

    rpc::RecoveryManifest merged;
    merged.CopyFrom(current);
    if (!MergeConfirmedHolder(*candidate,
                              proposed.version().generation(),
                              &merged)) {
      return false;
    }

    if (!commit_initial_frontier_prefix_if_ready(merged)) {
      return false;
    }
    update_committed_topology(merged);

    if (profiling_enabled_) {
      ++profile_.holder_admissions_committed;
      ++profile_.manifest_generations_committed;
      profile_.max_generation =
          std::max(profile_.max_generation, merged.version().generation());
      const uint64_t non_owner_holders =
          merged.succession_size() > 0
              ? static_cast<uint64_t>(merged.succession_size() - 1)
              : 0;
      profile_.max_non_owner_holders =
          std::max(profile_.max_non_owner_holders, non_owner_holders);
      if (merged.frozen()) {
        ++profile_.frozen_commits;
      }
    }

    committed_manifest->CopyFrom(merged);
    EraseHolderReservationLocked(reservation_id);
    return true;
  }

  // Patch 4D: only the next contiguous rank may become durable. Install RPCs
  // may complete in any order, but commits must remain H1,H2,...
  if (reservation.proposed_rank !=
          static_cast<uint32_t>(current.succession_size()) ||
      proposed.succession_size() != current.succession_size() + 1 ||
      proposed.version().generation() != current.version().generation() + 1) {
    return false;
  }

  for (int index = 0; index < current.succession_size(); ++index) {
    if (current.succession(index).SerializeAsString() !=
        proposed.succession(index).SerializeAsString()) {
      return false;
    }
  }

  if (!commit_initial_frontier_prefix_if_ready(proposed)) {
    return false;
  }
  update_committed_topology(proposed);

  if (profiling_enabled_) {
    ++profile_.holder_admissions_committed;
    ++profile_.manifest_generations_committed;

    if (proposed.version().generation() > profile_.max_generation) {
      profile_.max_generation = proposed.version().generation();
    }

    const uint64_t non_owner_holders =
        proposed.succession_size() > 0
            ? static_cast<uint64_t>(proposed.succession_size() - 1)
            : 0;

    if (non_owner_holders > profile_.max_non_owner_holders) {
      profile_.max_non_owner_holders = non_owner_holders;
    }

    if (proposed.frozen()) {
      ++profile_.frozen_commits;
    }
  }

  committed_manifest->CopyFrom(proposed);
  EraseHolderReservationLocked(reservation_id);
  return true;
}

void RecoverySuccessionManager::AbortHolderAdmission(
    const std::string &reservation_id) {
  if (reservation_id.empty()) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return;
  }

  const TaskID task_id = reservation_it->second.task_id;
  const uint32_t failed_rank = reservation_it->second.proposed_rank;


  // Patch 4M-CERT independent abort: another certificate does not depend on
  // this reservation's prefix, so do not invalidate higher slots.
  if (recovery_succession_certificate_admission_enabled_config_ &&
      !recovery_witness_holder_baseline_enabled_config_) {
    EraseHolderReservationLocked(reservation_id);
    return;
  }

  const auto task_index_it = holder_reservation_by_task_.find(task_id);
  if (task_index_it == holder_reservation_by_task_.end()) {
    holder_reservations_.erase(reservation_it);
    return;
  }

  // Patch 4D conservative failure rule: a missing lower rank invalidates every
  // speculative suffix reservation because their proposed manifests include it.
  std::vector<std::string> suffix;
  for (auto it = task_index_it->second.lower_bound(failed_rank);
       it != task_index_it->second.end(); ++it) {
    suffix.push_back(it->second);
  }

  for (const std::string &id : suffix) {
    EraseHolderReservationLocked(id);
  }
}

void RecoverySuccessionManager::AllowCandidateReportRetry(
    const TaskID &task_id) {
  if (task_id.IsNil()) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  candidate_reports_sent_.erase(task_id);
}


void RecoverySuccessionManager::EraseHolderReservationLocked(
    const std::string &reservation_id) {
  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return;
  }

  const TaskID task_id = reservation_it->second.task_id;
  const uint32_t rank = reservation_it->second.proposed_rank;

  const auto task_index_it = holder_reservation_by_task_.find(task_id);
  if (task_index_it != holder_reservation_by_task_.end()) {
    auto rank_it = task_index_it->second.find(rank);
    if (rank_it != task_index_it->second.end() && rank_it->second == reservation_id) {
      task_index_it->second.erase(rank_it);
    }
    if (task_index_it->second.empty()) {
      holder_reservation_by_task_.erase(task_index_it);
    }
  }

  holder_reservations_.erase(reservation_it);
}



bool RecoverySuccessionManager::ApplyCommittedManifest(
    const rpc::RecoveryManifest &manifest) {
  if (manifest.task_id().empty()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(manifest.task_id());
  absl::MutexLock lock(&mutex_);

  if (InitialFrontierPiggybackEnabledCached() && !manifest.tombstoned()) {
    const auto terminal = task_states_.find(task_id);
    if (terminal != task_states_.end() && terminal->second.manifest.tombstoned()) {
      return false;
    }
  }

  const bool frontier_group_manifest =
      AdaptiveRecoveryFrontierEnabledCached() &&
      recovery_frontier_planner_->GetGroup(task_id) != nullptr;

  auto apply_topology =
      [this, &task_id, frontier_group_manifest](
          const rpc::RecoveryManifest &group_or_task_manifest,
          bool committed) {
        if (!frontier_group_manifest) {
          UpdateManifestForTaskLocked(task_id, group_or_task_manifest, committed);
          return;
        }

        const RecoveryFrontierGroup *group =
            recovery_frontier_planner_->GetGroup(task_id);
        RAY_CHECK(group != nullptr);
        recovery_frontier_protection_manifests_[task_id].CopyFrom(
            group_or_task_manifest);
        for (const RecoveryFrontierMember &member : group->Members()) {
          rpc::RecoveryManifest member_manifest =
              recovery_succession_internal::BuildFrontierMemberManifest(
                  group_or_task_manifest, member);
          UpdateManifestForTaskLocked(member.task_id, member_manifest, committed);
        }
      };

  if (recovery_succession_certificate_admission_enabled_config_ &&
      !recovery_witness_holder_baseline_enabled_config_) {
    // Patch 4M-CERT set merge: equal-generation different subsets are valid
    // partial views and must converge by union, not fail byte-equality checks.
    auto it = task_states_.find(task_id);
    rpc::RecoveryManifest merged;
    if (it == task_states_.end() || it->second.manifest.task_id().empty()) {
      merged.CopyFrom(manifest);
    } else {
      merged.CopyFrom(it->second.manifest);
      if (!MergeRecoveryHolderSets(manifest, &merged)) {
        return false;
      }
    }
    apply_topology(merged, true);
    if (ContainsWorker(merged, self_address_)) {
      candidate_reports_sent_.insert(task_id);
    }
    return true;
  }

  const auto task_it = task_states_.find(task_id);

  if (task_it != task_states_.end() && !task_it->second.manifest.task_id().empty()) {
    const int comparison = CompareManifestVersions(manifest, task_it->second.manifest);

    if (comparison < 0) {
      // Patch 4D failure-only rollback. A speculative higher-rank holder may
      // have installed a future manifest before a lower rank failed. The
      // coordinator cleans that candidate up by sending the last committed
      // prefix through the existing CommitRecoveryManifest RPC. Accept this
      // older prefix only for an uncommitted provisional holder that is NOT a
      // member of the committed prefix.
      if (!task_it->second.manifest_committed &&
          !ContainsWorker(manifest, self_address_)) {
        apply_topology(manifest, true);
        candidate_reports_sent_.erase(task_id);
        return true;
      }
      return false;
    }

    if (comparison == 0 &&
        manifest.SerializeAsString() != task_it->second.manifest.SerializeAsString()) {
      return false;
    }
  }

  apply_topology(manifest, true);

  if (ContainsWorker(manifest, self_address_)) {
    candidate_reports_sent_.insert(task_id);
  }

  return true;
}



bool RecoverySuccessionManager::BuildRecoveryMetadataLocked(
    const ObjectID &object_id,
    rpc::RecoveryObjectMetadata *metadata,
    bool require_frontier_commit) const {
  if (object_id.IsNil()) {
    return false;
  }

  const TaskID task_id = object_id.TaskId();
  uint32_t return_index = 0;
  bool known_object = false;

  const auto borrowed_it = borrowed_objects_.find(object_id);
  if (borrowed_it != borrowed_objects_.end() &&
      borrowed_it->second.task_id == task_id) {
    return_index = borrowed_it->second.return_index;
    known_object = true;
  }

  const auto task_it = task_states_.find(task_id);
  if (!known_object && task_it != task_states_.end() &&
      task_it->second.owned_num_returns > 0) {
    const auto object_index = object_id.ObjectIndex();
    if (object_index > 0 &&
        static_cast<uint64_t>(object_index) <=
            static_cast<uint64_t>(task_it->second.owned_num_returns)) {
      return_index = static_cast<uint32_t>(object_index - 1);
      known_object = true;
    }
  }

  if (known_object && task_it != task_states_.end() &&
      !task_it->second.manifest.task_id().empty()) {
    // Fixed-R Recovery Frontier metadata is an acknowledged-prefix capability.
    // The owner may construct task-local recovery state before the fixed-R
    // append completes, but that state must remain invisible to exporters
    // until the member's replay recipe is committed on every fixed-R holder.
    // Adaptive Succession is different: candidate formation requires metadata
    // to reach the first borrower before any non-owner holder can be admitted.
    // Keep this visibility gate strictly scoped to the frozen Fixed-R backend.
    if (require_frontier_commit &&
        recovery_witness_holder_baseline_enabled_config_ &&
        recovery_frontier_planner_ != nullptr &&
        recovery_frontier_planner_->GroupSize() > 1) {
      const auto membership = recovery_frontier_planner_->FindTask(task_id);
      if (membership.has_value()) {
        const RecoveryFrontierGroup *group =
            recovery_frontier_planner_->GetGroup(membership->group_id);
        if (group == nullptr || !group->IsTaskCommitted(task_id)) {
          return false;
        }
      }
    }

    if (AdaptiveRecoveryFrontierEnabledCached()) {
      const auto membership = recovery_frontier_planner_->FindTask(task_id);
      if (membership.has_value()) {
        const RecoveryFrontierGroup *group =
            recovery_frontier_planner_->GetGroup(membership->group_id);
        const auto leader_it = task_states_.find(membership->group_id);
        if (group != nullptr && leader_it != task_states_.end() &&
            !leader_it->second.manifest.task_id().empty() &&
            membership->member_index < group->Members().size()) {
          const auto protection_it =
              recovery_frontier_protection_manifests_.find(
                  membership->group_id);
          const bool adaptive_topology_established =
              protection_it != recovery_frontier_protection_manifests_.end() &&
              protection_it->second.frozen();
          if (require_frontier_commit && adaptive_topology_established &&
              !group->IsTaskCommitted(task_id)) {
            return false;
          }

          const RecoveryFrontierMember &member =
              group->Members()[membership->member_index];
          if (member.task_id == task_id) {
            if (metadata != nullptr) {
              metadata->Clear();
              metadata->set_task_id(task_id.Binary());
              metadata->set_return_index(return_index);
              rpc::RecoveryManifest member_manifest =
                  recovery_succession_internal::BuildFrontierMemberManifest(
                      leader_it->second.manifest, member);
              metadata->mutable_manifest()->CopyFrom(member_manifest);
              metadata->set_first_holder_task_spec(
                  recovery_succession_internal::EncodeFrontierSuccessionMemberMarker(
                      membership->group_id));
            }
            if (profiling_enabled_) {
              ++profile_.task_centric_metadata_builds;
            }
            return true;
          }
        }
      }
    }

    if (metadata != nullptr) {
      metadata->Clear();
      metadata->set_task_id(task_id.Binary());
      metadata->set_return_index(return_index);
      metadata->mutable_manifest()->CopyFrom(task_it->second.manifest);
    }
    if (profiling_enabled_) {
      ++profile_.task_centric_metadata_builds;
    }
    return true;
  }

  const auto legacy_it = object_recovery_metadata_.find(object_id);
  if (legacy_it == object_recovery_metadata_.end()) {
    return false;
  }
  if (metadata != nullptr) {
    metadata->CopyFrom(legacy_it->second);
  }
  return true;
}


bool RecoverySuccessionManager::HasRecoveryMetadata(
    const ObjectID &object_id) const {
  absl::MutexLock lock(&mutex_);
  return BuildRecoveryMetadataLocked(object_id, nullptr, /*require_frontier_commit=*/true);
}


bool RecoverySuccessionManager::PopulateRecoveryMetadata(
    const ObjectID &object_id, rpc::RecoveryObjectMetadata *metadata) const {
  if (metadata == nullptr) {
    return false;
  }

  const auto patch4g_start = std::chrono::steady_clock::now();
  absl::MutexLock lock(&mutex_);

  const bool hit = BuildRecoveryMetadataLocked(
      object_id, metadata, /*require_frontier_commit=*/true);

  if (profiling_enabled_) {
    ++profile_.recovery_metadata_lookup_calls;
    if (hit) {
      ++profile_.recovery_metadata_lookup_hits;
    }
    profile_.recovery_metadata_lookup_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
  }

  return hit;
}

void RecoverySuccessionManager::PopulateTaskArgumentMetadata(
    rpc::TaskSpec *task_spec) {
  PopulateTaskArgumentMetadataInternal(
      task_spec, /*require_frontier_commit=*/true);
}

void RecoverySuccessionManager::PopulateTaskArgumentMetadataForDeferredFrontierDispatch(
    rpc::TaskSpec *task_spec) {
  PopulateTaskArgumentMetadataInternal(
      task_spec, /*require_frontier_commit=*/false);
}

void RecoverySuccessionManager::PopulateTaskArgumentMetadataInternal(
    rpc::TaskSpec *task_spec, bool require_frontier_commit) {
  if (task_spec == nullptr) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  // This field is transport-only. Rebuilding it from manager state also makes
  // this method idempotent if task construction revisits the same TaskSpec.
  task_spec->clear_recovery_argument_metadata();
  absl::flat_hash_set<ObjectID> attached_object_ids;

  auto populate_one = [this, task_spec, &attached_object_ids, require_frontier_commit](
                          const ObjectID &object_id,
                          rpc::ObjectReference *object_ref) {
    if (object_ref == nullptr || object_id.IsNil()) {
      return;
    }

    // A legacy/pre-4I ObjectRef may already carry recovery metadata. Keep it
    // in place while checking authoritative manager state so the common path
    // does not deep-copy a protobuf that will immediately be discarded. If
    // manager state misses, expand the compatibility fallback directly from
    // the ObjectReference. In every case, clear the per-ref transport field
    // before the TaskSpec continues to the wire.
    const bool had_legacy_transport = object_ref->has_recovery_metadata();

    // One sidecar per unique dependency even if the same ObjectRef appears in
    // multiple direct/nested argument positions. Duplicate legacy metadata is
    // still stripped from the ordinary ObjectReference wire path.
    if (attached_object_ids.contains(object_id)) {
      object_ref->clear_recovery_metadata();
      return;
    }

    rpc::RecoveryObjectMetadata source_storage;
    rpc::RecoveryObjectMetadata legacy_expanded;
    const rpc::RecoveryObjectMetadata *source = nullptr;

    if (BuildRecoveryMetadataLocked(
            object_id, &source_storage, require_frontier_commit)) {
      source = &source_storage;
    } else if (had_legacy_transport &&
               ExpandTaskArgumentRecoveryMetadata(*object_ref, &legacy_expanded)) {
      source = &legacy_expanded;
    }

    // Recovery metadata is carried only in the TaskSpec-level Patch-4I sidecar
    // after this point, regardless of whether manager state or the legacy
    // compatibility fallback supplied it.
    object_ref->clear_recovery_metadata();

    if (source == nullptr || source->task_id().empty() || !source->has_manifest()) {
      return;
    }

    rpc::RecoveryTaskArgumentMetadata *entry =
        task_spec->add_recovery_argument_metadata();
    entry->set_object_id(object_id.Binary());
    if (object_ref->has_owner_address()) {
      entry->mutable_owner_address()->CopyFrom(object_ref->owner_address());
    }

    rpc::RecoveryObjectMetadata *out = entry->mutable_recovery_metadata();
    bool compact_transport = false;

    if (entry->has_owner_address()) {
      compact_transport =
          recovery_succession_internal::WriteCompactTaskArgumentRecoveryMetadata(
              *source, source->manifest(), entry->owner_address(), out);
      if (!compact_transport) {
        out->CopyFrom(*source);
        recovery_succession_internal::ClearFirstHolderPayloadUnlessFrontierMembership(
            out);
        out->clear_compact_manifest();
      }
    } else {
      // Original baseline representation or safety fallback when the owner
      // address cannot reconstruct rank 0. Preserve only a Frontier membership
      // marker; ordinary first-holder TaskSpec piggybacks remain transport-only.
      out->CopyFrom(*source);
      recovery_succession_internal::ClearFirstHolderPayloadUnlessFrontierMembership(
          out);
      out->clear_compact_manifest();
    }

    if (InitialFrontierPiggybackEnabledCached() && compact_transport) {
      const TaskID group_id = object_id.TaskId();
      const auto *group = recovery_frontier_planner_->GetGroup(group_id);
      const auto *owner = FindHolderByRank(source->manifest(), 0);
      const auto initial = adaptive_frontier_initial_append_batches_.find(group_id);
      // No send quota: repeated exports to one worker cannot consume another
      // failure domain's chance to receive a recipe. Non-leader/partial-group
      // exports keep the existing installation path and never wait for filling.
      if (group != nullptr && group->Full() &&
          group->MemberCount() == recovery_frontier_group_size_config_ &&
          group->Generation() == 0 && group->CommittedMemberCount() == 0 &&
          owner != nullptr && SameWorker(owner->address(), self_address_) &&
          !source->manifest().tombstoned() && !source->manifest().frozen() &&
          source->manifest().target_holder_count() ==
              recovery_succession_target_holder_count_config_ &&
          source->manifest().witness_count() == recovery_succession_witness_count_config_ &&
          static_cast<uint32_t>(source->manifest().witness_raylets_size()) ==
              source->manifest().witness_count() &&
          (initial == adaptive_frontier_initial_append_batches_.end() ||
           (initial->second.begin_member_index == 0 &&
            initial->second.end_member_index == group->MemberCount()))) {
        const auto build_start =
            profiling_enabled_ ? std::chrono::steady_clock::now()
                               : std::chrono::steady_clock::time_point{};
        rpc::RecoveryFrontierAppend snapshot;
        if (recovery_succession_internal::BuildFrontierSuccessionSnapshot(
                *group, &snapshot) &&
            snapshot.SerializeToString(entry->mutable_initial_frontier_recipe())) {
          if (profiling_enabled_) {
            const uint64_t build_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - build_start).count();
            const uint64_t bytes = entry->initial_frontier_recipe().size();
            ++profile_.frontier_recipe_piggybacks_sent;
            profile_.frontier_recipe_piggyback_bytes_sent += bytes;
            ++profile_.frontier_recipe_encode_calls;
            profile_.frontier_recipe_encode_time_ns += build_ns;
            profile_.frontier_recipe_encode_members += group->MemberCount();
            profile_.frontier_recipe_encode_bytes += bytes;
            profile_.task_spec_bytes_sent += bytes;
          }
        } else {
          entry->clear_initial_frontier_recipe();
        }
      }
    }

    // Ordinary adaptive K=1 holder fast path. Lazy activation staged one
    // sanitized producer TaskSpec in the existing task state. The first R
    // downstream metadata builds may transport it in the already-supported
    // Patch-4F field; the duplicate is released after R sends. Every
    // receiver remains provisional and must still verify witness durability.
    if (!recovery_witness_holder_baseline_enabled_config_ &&
        !recovery_frontier_enabled_config_ &&
        !recovery_succession_certificate_admission_enabled_config_) {
      const TaskID producer_task_id = object_id.TaskId();
      const auto producer_it = task_states_.find(producer_task_id);
      if (producer_it != task_states_.end()) {
        TaskRecoveryState &state = producer_it->second;
        if (state.manifest_committed && state.manifest.target_holder_count() > 0 &&
            state.first_holder_piggybacks_sent < state.manifest.target_holder_count() &&
            !state.manifest.tombstoned() &&
            !state.manifest.frozen() &&
            state.manifest.task_id() == source->task_id() &&
            state.task_spec.has_value() &&
            state.task_spec->task_id() == source->task_id()) {
          const auto piggyback_start = std::chrono::steady_clock::now();

          rpc::TaskSpec piggyback_task_spec;
          piggyback_task_spec.CopyFrom(state.task_spec.value());
          ClearFirstHolderTaskSpecPiggybacks(&piggyback_task_spec);
          piggyback_task_spec.mutable_recovery_manifest()->CopyFrom(state.manifest);

          std::string serialized_task_spec;
          if (piggyback_task_spec.SerializeToString(&serialized_task_spec) &&
              !serialized_task_spec.empty()) {
            out->set_first_holder_task_spec(serialized_task_spec);

            // A repeated export can go to the same worker. This bounded send
            // budget never counts as holder admission or distinct-node proof.
            ++state.first_holder_piggybacks_sent;
            if (state.first_holder_piggybacks_sent >= state.manifest.target_holder_count()) {
              state.task_spec.reset();
            }

            if (profiling_enabled_) {
              const uint64_t piggyback_ns = static_cast<uint64_t>(
                  std::chrono::duration_cast<std::chrono::nanoseconds>(
                      std::chrono::steady_clock::now() - piggyback_start)
                      .count());
              const uint64_t piggyback_bytes =
                  static_cast<uint64_t>(serialized_task_spec.size());
              ++profile_.first_holder_piggyback_copies_sent;
              profile_.first_holder_piggyback_bytes_sent += piggyback_bytes;
              profile_.first_holder_piggyback_serialize_time_ns += piggyback_ns;
              profile_.task_spec_bytes_sent += piggyback_bytes;
            }
          }
        }
      }
    }

    attached_object_ids.insert(object_id);

    if (profiling_enabled_) {
      ++profile_.task_argument_metadata_refs_attached;
      profile_.task_argument_metadata_full_bytes_equivalent +=
          static_cast<uint64_t>(source->ByteSizeLong());
      profile_.task_argument_metadata_transport_bytes +=
          static_cast<uint64_t>(out->ByteSizeLong());
      if (compact_transport) {
        ++profile_.task_argument_metadata_compact_refs;
      } else {
        ++profile_.task_argument_metadata_compact_fallbacks;
      }
    }

  };

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && !arg.object_ref().object_id().empty() &&
        arg.object_ref().object_id().size() == ObjectID::Size()) {
      populate_one(ObjectID::FromBinary(arg.object_ref().object_id()),
                   arg.mutable_object_ref());
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.object_id().empty() ||
          nested_ref.object_id().size() != ObjectID::Size()) {
        continue;
      }
      populate_one(ObjectID::FromBinary(nested_ref.object_id()), &nested_ref);
    }
  }
}

void RecoverySuccessionManager::MaybeAddCandidateReportLocked(
    const rpc::RecoveryManifest &manifest,
    bool already_stores_task_spec,
    std::vector<CandidateReport> *reports) {
  const auto patch4g_start = std::chrono::steady_clock::now();
  const size_t patch4g_reports_before = reports == nullptr ? 0 : reports->size();
  absl::Cleanup patch4g_profile = [this, patch4g_start, patch4g_reports_before, reports] {
    if (!profiling_enabled_) {
      return;
    }
    ++profile_.candidate_report_build_calls;
    profile_.candidate_report_build_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
    if (reports != nullptr && reports->size() > patch4g_reports_before) {
      profile_.candidate_reports_built +=
          static_cast<uint64_t>(reports->size() - patch4g_reports_before);
    }
  };

  if (recovery_succession_enabled_config_ &&
      recovery_witness_holder_baseline_enabled_config_) {
    return;
  }

  if (reports == nullptr || manifest.task_id().empty()) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(manifest.task_id());

  if (candidate_reports_sent_.contains(task_id)) {
    return;
  }

  // The caller's manifest already lives for the duration of this function.
  // Point at it unless manager state is newer instead of deep-copying a full
  // protobuf only to copy the selected view once more into the outgoing RPC.
  const rpc::RecoveryManifest *effective_manifest = &manifest;

  const auto task_it = task_states_.find(task_id);

  if (task_it != task_states_.end() && !task_it->second.manifest.task_id().empty() &&
      CompareManifestVersions(task_it->second.manifest, *effective_manifest) > 0) {
    effective_manifest = &task_it->second.manifest;
  }

  if (effective_manifest->frozen() || effective_manifest->tombstoned() ||
      ContainsWorker(*effective_manifest, self_address_)) {
    return;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(*effective_manifest, 0);

  if (owner == nullptr || SameWorker(owner->address(), self_address_)) {
    return;
  }

  CandidateReport report;
  report.coordinator_address.CopyFrom(owner->address());

  report.request.set_task_id(effective_manifest->task_id());
  report.request.mutable_candidate_address()->CopyFrom(self_address_);
  report.request.mutable_cached_manifest()->CopyFrom(*effective_manifest);
  report.request.set_already_stores_task_spec(already_stores_task_spec);

  reports->push_back(std::move(report));

  candidate_reports_sent_.insert(task_id);
}

void RecoverySuccessionManager::UpdateManifestForTaskLocked(
    const TaskID &task_id,
    const rpc::RecoveryManifest &manifest,
    bool committed) {
  TaskRecoveryState &task_state = task_states_[task_id];

  const bool discard_unadmitted_piggyback =
      committed &&
      task_state.provisional_piggyback_task_spec &&
      !ContainsWorker(manifest, self_address_);

  task_state.manifest.CopyFrom(manifest);
  task_state.manifest_committed = committed;

  if (committed) {
    task_state.provisional_reservation_id.clear();

    if (task_state.provisional_piggyback_task_spec) {
      task_state.provisional_piggyback_task_spec = false;

      if (discard_unadmitted_piggyback) {
        // Rollback/rejection must not leave an orphaned full TaskSpec that can
        // later be mistaken for committed recovery lineage.
        task_state.task_spec.reset();
        candidate_reports_sent_.erase(task_id);
      }
    }
  }

  // Do not copy the new manifest into the stored TaskSpec here.
  // PrepareHolderAdmission and PrepareTaskReplay attach the current manifest
  // immediately before the TaskSpec is transferred or replayed.

  const auto object_ids_it = task_object_ids_.find(task_id);

  if (object_ids_it == task_object_ids_.end()) {
    return;
  }

  for (const ObjectID &object_id : object_ids_it->second) {
    const auto metadata_it = object_recovery_metadata_.find(object_id);

    if (metadata_it != object_recovery_metadata_.end()) {
      metadata_it->second.mutable_manifest()->CopyFrom(manifest);
      metadata_it->second.clear_first_holder_task_spec();
    }
  }
}


bool RecoverySuccessionManager::GetBorrowedObjectRecoveryPlan(
    const ObjectID &object_id, BorrowedObjectRecoveryPlan *plan) const {
  if (plan == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto borrowed_it = borrowed_objects_.find(object_id);
  if (borrowed_it == borrowed_objects_.end()) {
    return false;
  }

  const auto task_it = task_states_.find(borrowed_it->second.task_id);
  if (task_it == task_states_.end() ||
      task_it->second.manifest.task_id().empty()) {
    return false;
  }

  plan->task_id = borrowed_it->second.task_id;
  plan->return_index = borrowed_it->second.return_index;
  plan->cached_manifest.CopyFrom(task_it->second.manifest);
  return true;
}

RecoverySuccessionManager::ReplayPreparationResult
RecoverySuccessionManager::PrepareTaskReplay(
    const rpc::RecoverTaskOutputRequest &request,
    const rpc::TaskSpec *owner_task_spec,
    rpc::TaskSpec *task_spec,
    rpc::RecoveryManifest *latest_manifest) {
  if (task_spec == nullptr ||
    latest_manifest == nullptr ||
    request.task_id().size() != TaskID::Size() ||
    !request.has_requester_manifest() ||
    request.requester_manifest().task_id() !=
        request.task_id()) {
    return ReplayPreparationResult::TASK_NOT_FOUND;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());

  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    return ReplayPreparationResult::TASK_NOT_FOUND;
  }

  TaskRecoveryState &state = task_it->second;
  latest_manifest->CopyFrom(state.manifest);

  if (state.manifest.tombstoned()) {
    return ReplayPreparationResult::TOMBSTONED;
  }

  const rpc::TaskSpec *lineage_task_spec = nullptr;

  if (state.task_spec.has_value()) {
    lineage_task_spec = &state.task_spec.value();
  } else if (owner_task_spec != nullptr) {
    lineage_task_spec = owner_task_spec;
  } else {
    // Patch 4L: owner replay may outlive TaskManager's ordinary lineage entry.
    const auto retained_it = owner_retained_tasks_.find(task_id);
    if (retained_it != owner_retained_tasks_.end() &&
        retained_it->second.HasLiveReturns() &&
        !retained_it->second.task_spec.task_id().empty()) {
      lineage_task_spec = &retained_it->second.task_spec;
    }
  }

  if (lineage_task_spec == nullptr ||
      lineage_task_spec->task_id() != task_id.Binary() ||
      !IsEligibleTask(*lineage_task_spec) ||
      request.return_index() >= static_cast<uint32_t>(lineage_task_spec->num_returns())) {
    return ReplayPreparationResult::TASK_NOT_FOUND;
  }

  if (!state.manifest_committed) {
    // Both an installed holder and a Patch-4F piggyback holder remain
    // non-replayable until this worker independently verifies witnesses.
    const bool installed_provisional =
        !state.provisional_reservation_id.empty() &&
        ContainsWorker(state.manifest, self_address_);
    const bool piggyback_provisional =
        state.provisional_piggyback_task_spec;

    if (!installed_provisional && !piggyback_provisional) {
      return ReplayPreparationResult::TASK_NOT_FOUND;
    }

    return ReplayPreparationResult::WITNESS_CONFIRMATION_REQUIRED;
  }

  if (recovery_succession_certificate_admission_enabled_config_ &&
      !recovery_witness_holder_baseline_enabled_config_) {
    rpc::RecoveryManifest merged;
    merged.CopyFrom(state.manifest);
    if (!MergeRecoveryHolderSets(request.requester_manifest(), &merged)) {
      return ReplayPreparationResult::MANIFEST_STALE;
    }
    if (merged.tombstoned()) {
      latest_manifest->CopyFrom(merged);
      return ReplayPreparationResult::TOMBSTONED;
    }
    state.manifest.CopyFrom(merged);
    state.manifest_committed = true;
    latest_manifest->CopyFrom(state.manifest);
  } else {
    const int requester_comparison =
      CompareManifestVersions(
          request.requester_manifest(),
          state.manifest);

    if (requester_comparison < 0) {
      return ReplayPreparationResult::MANIFEST_STALE;
    }

    if (requester_comparison == 0 &&
        request.requester_manifest().SerializeAsString() !=
            state.manifest.SerializeAsString()) {
      return ReplayPreparationResult::MANIFEST_STALE;
    }

    if (requester_comparison > 0) {
      if (request.requester_manifest().tombstoned()) {
        latest_manifest->CopyFrom(
            request.requester_manifest());
        return ReplayPreparationResult::TOMBSTONED;
      }

      if (!ContainsWorker(
              request.requester_manifest(),
              self_address_)) {
        return ReplayPreparationResult::WRONG_HOLDER;
      }

      state.manifest.CopyFrom(
          request.requester_manifest());
      state.manifest_committed = true;
      latest_manifest->CopyFrom(
          state.manifest);
    }
  }

  bool self_is_holder = false;

  for (const rpc::RecoveryHolder &holder : state.manifest.succession()) {
    if (SameWorker(holder.address(), self_address_)) {
      self_is_holder = true;
      break;
    }
  }

  if (!self_is_holder) {
    return ReplayPreparationResult::WRONG_HOLDER;
  }

  // RegisterOwnedTask records all static returns synchronously before the replay
  // is submitted. Concurrent/late requesters must follow that acting owner,
  // rather than register the same deterministic returns and TaskID again.
  // Keep witness, manifest, tombstone and holder checks above this fast path.
  if (state.owned_num_returns > 0) {
    task_spec->CopyFrom(*lineage_task_spec);
    return ReplayPreparationResult::ALREADY_OWNED;
  }

  const int32_t max_recovery_attempts = state.manifest.max_recovery_attempts();

  if (max_recovery_attempts >= 0 &&
      state.manifest.recovery_attempt() >= static_cast<uint32_t>(max_recovery_attempts)) {
    return ReplayPreparationResult::RETRY_LIMIT_EXCEEDED;
  }

  state.manifest.set_recovery_attempt(state.manifest.recovery_attempt() + 1);

  task_spec->CopyFrom(*lineage_task_spec);
  ClearFirstHolderTaskSpecPiggybacks(task_spec);
  task_spec->mutable_recovery_manifest()->CopyFrom(state.manifest);
  latest_manifest->CopyFrom(state.manifest);

  return ReplayPreparationResult::READY;
}



bool RecoverySuccessionManager::ConfirmProvisionalHolderFromWitness(
    const TaskID &task_id,
    const rpc::RecoveryManifest &witness_manifest,
    rpc::RecoveryManifest *confirmed_manifest) {
  if (task_id.IsNil() || confirmed_manifest == nullptr ||
      witness_manifest.task_id().size() != TaskID::Size() ||
      !witness_manifest.has_version() ||
      witness_manifest.tombstoned()) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  // Ordinary Succession stores one witness record per task. Adaptive Frontier
  // Succession stores the shared topology under the group/leader TaskID. The
  // holder imported the Frontier snapshot before the owner published that
  // witness generation, so a matching witness-backed group manifest can be
  // translated safely into the requested member's task-local manifest.
  rpc::RecoveryManifest task_witness_manifest;
  task_witness_manifest.CopyFrom(witness_manifest);

  if (AdaptiveRecoveryFrontierEnabledCached()) {
    const auto membership = recovery_frontier_planner_->FindTask(task_id);
    if (membership.has_value()) {
      if (witness_manifest.task_id() != membership->group_id.Binary()) {
        return false;
      }

      const RecoveryFrontierGroup *group =
          recovery_frontier_planner_->GetGroup(membership->group_id);
      if (group == nullptr ||
          membership->member_index >= group->Members().size()) {
        return false;
      }

      const RecoveryFrontierMember &member =
          group->Members()[membership->member_index];
      if (member.task_id != task_id) {
        return false;
      }

      task_witness_manifest =
          recovery_succession_internal::BuildFrontierMemberManifest(
              witness_manifest, member);
    } else if (witness_manifest.task_id() != task_id.Binary()) {
      return false;
    }
  } else if (witness_manifest.task_id() != task_id.Binary()) {
    return false;
  }

  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end() ||
      !task_it->second.task_spec.has_value()) {
    return false;
  }

  TaskRecoveryState &state = task_it->second;

  if (state.manifest.task_id() != task_id.Binary() ||
      task_witness_manifest.task_id() != task_id.Binary() ||
      state.manifest.tombstoned() ||
      !ContainsWorker(task_witness_manifest, self_address_)) {
    return false;
  }


  if (recovery_succession_certificate_admission_enabled_config_ &&
      !recovery_witness_holder_baseline_enabled_config_) {
    // Patch 4M-CERT witness set promotion. Presence in a directly queried
    // witness's merged set is the durability proof; rank/prefix is irrelevant.
    const bool installed_provisional =
        !state.provisional_reservation_id.empty() &&
        ContainsWorker(state.manifest, self_address_);
    const bool piggyback_provisional = state.provisional_piggyback_task_spec;
    if (!state.manifest_committed &&
        !installed_provisional && !piggyback_provisional) {
      return false;
    }

    rpc::RecoveryManifest merged;
    merged.CopyFrom(state.manifest);
    if (!MergeRecoveryHolderSets(task_witness_manifest, &merged) ||
        !ContainsWorker(merged, self_address_)) {
      return false;
    }
    UpdateManifestForTaskLocked(task_id, merged, true);
    candidate_reports_sent_.insert(task_id);
    confirmed_manifest->CopyFrom(task_states_[task_id].manifest);
    return true;
  }

  const bool installed_provisional =
      !state.provisional_reservation_id.empty() &&
      ContainsWorker(state.manifest, self_address_);
  const bool piggyback_provisional =
      state.provisional_piggyback_task_spec;

  // A normal committed holder must already appear in its local manifest.
  // Patch 4F is intentionally different only while provisional: H1 initially
  // has [A] locally, and may promote only if a directly fetched witness
  // manifest contains this worker.
  if (state.manifest_committed &&
      !ContainsWorker(state.manifest, self_address_)) {
    return false;
  }

  if (!state.manifest_committed &&
      !installed_provisional &&
      !piggyback_provisional) {
    return false;
  }

  const int comparison =
      CompareManifestVersions(task_witness_manifest, state.manifest);

  if (comparison < 0) {
    if (!state.manifest_committed) {
      return false;
    }

    confirmed_manifest->CopyFrom(state.manifest);
    return true;
  }

  if (comparison == 0 &&
      task_witness_manifest.SerializeAsString() !=
          state.manifest.SerializeAsString()) {
    return false;
  }

  if (comparison > 0 || !state.manifest_committed) {
    UpdateManifestForTaskLocked(task_id, task_witness_manifest, true);
  }

  candidate_reports_sent_.insert(task_id);
  confirmed_manifest->CopyFrom(state.manifest);
  return true;
}


void RecoverySuccessionManager::UpdateBorrowedObjectManifest(
    const ObjectID &object_id,
    const rpc::RecoveryManifest &manifest) {
  if (manifest.task_id().empty()) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  const auto borrowed_it = borrowed_objects_.find(object_id);
  if (borrowed_it == borrowed_objects_.end() ||
      borrowed_it->second.task_id.Binary() != manifest.task_id()) {
    return;
  }


  if (recovery_succession_certificate_admission_enabled_config_ &&
      !recovery_witness_holder_baseline_enabled_config_) {
    // Patch 4M-CERT borrowed-view merge.
    const TaskID task_id = borrowed_it->second.task_id;
    auto task_it = task_states_.find(task_id);
    rpc::RecoveryManifest merged;
    if (task_it == task_states_.end() || task_it->second.manifest.task_id().empty()) {
      merged.CopyFrom(manifest);
    } else {
      merged.CopyFrom(task_it->second.manifest);
      if (!MergeRecoveryHolderSets(manifest, &merged)) {
        return;
      }
    }
    UpdateManifestForTaskLocked(task_id, merged, true);
    return;
  }

  TaskRecoveryState &state = task_states_[borrowed_it->second.task_id];
  if (!state.manifest.task_id().empty() &&
      CompareManifestVersions(manifest, state.manifest) < 0) {
    return;
  }

  state.manifest.CopyFrom(manifest);
}

std::optional<rpc::RecoveryManifest> RecoverySuccessionManager::BuildTombstoneForTask(
    const TaskID &task_id) const {
  absl::MutexLock lock(&mutex_);

  if (recovery_frontier_planner_ != nullptr &&
      recovery_frontier_planner_->GroupSize() > 1) {
    const auto membership = recovery_frontier_planner_->FindTask(task_id);
    if (membership.has_value()) {
      const RecoveryFrontierGroup *group =
          recovery_frontier_planner_->GetGroup(membership->group_id);
      if (group == nullptr) {
        return std::nullopt;
      }

      // A group tombstone is legal only when every registered member has
      // lost its final owner return. This protects live siblings from an
      // early leader/member deletion.
      for (const RecoveryFrontierMember &member : group->Members()) {
        const auto live_it = owner_retained_tasks_.find(member.task_id);
        if (live_it != owner_retained_tasks_.end() &&
            live_it->second.HasLiveReturns()) {
          return std::nullopt;
        }
      }

      const auto protection_it =
          recovery_frontier_protection_manifests_.find(membership->group_id);
      if (protection_it == recovery_frontier_protection_manifests_.end()) {
        return std::nullopt;
      }

      const rpc::RecoveryHolder *owner =
          FindHolderByRank(protection_it->second, 0);
      if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
        return std::nullopt;
      }

      rpc::RecoveryManifest tombstone;
      tombstone.CopyFrom(protection_it->second);
      tombstone.set_task_id(membership->group_id.Binary());
      tombstone.set_tombstoned(true);
      tombstone.set_frozen(true);
      tombstone.mutable_version()->set_generation(
          protection_it->second.version().generation() + 1);
      return tombstone;
    }
  }

  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    return std::nullopt;
  }

  const TaskRecoveryState &task_state = task_it->second;

  if (!task_state.manifest_committed || task_state.manifest.tombstoned()) {
    return std::nullopt;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(task_state.manifest, 0);

  if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
    return std::nullopt;
  }

  rpc::RecoveryManifest tombstone;
  tombstone.CopyFrom(task_state.manifest);
  tombstone.set_tombstoned(true);
  tombstone.set_frozen(true);
  tombstone.mutable_version()->set_generation(
      task_state.manifest.version().generation() + 1);
  return tombstone;
}
bool RecoverySuccessionManager::ApplyRecoveryTombstone(
    const rpc::RecoveryManifest &tombstone) {
  if (tombstone.task_id().size() != TaskID::Size() || !tombstone.has_version() ||
      !tombstone.tombstoned()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(tombstone.task_id());

  absl::MutexLock lock(&mutex_);

  if (recovery_frontier_planner_ != nullptr &&
      recovery_frontier_planner_->GroupSize() > 1) {
    const RecoveryFrontierGroup *group =
        recovery_frontier_planner_->GetGroup(task_id);
    const auto protection_it =
        recovery_frontier_protection_manifests_.find(task_id);
    if (group != nullptr &&
        protection_it != recovery_frontier_protection_manifests_.end()) {
      if (CompareManifestVersions(tombstone, protection_it->second) <= 0) {
        return false;
      }

      std::vector<TaskID> member_task_ids;
      member_task_ids.reserve(group->Members().size());
      for (const RecoveryFrontierMember &member : group->Members()) {
        member_task_ids.push_back(member.task_id);
      }

      for (const TaskID &member_task_id : member_task_ids) {
        EraseTaskObjectMetadataLocked(member_task_id);

        const auto reservation_it =
            holder_reservation_by_task_.find(member_task_id);
        if (reservation_it != holder_reservation_by_task_.end()) {
          std::vector<std::string> reservation_ids;
          reservation_ids.reserve(reservation_it->second.size());
          for (const auto &[rank, reservation_id] : reservation_it->second) {
            static_cast<void>(rank);
            reservation_ids.push_back(reservation_id);
          }
          for (const std::string &reservation_id : reservation_ids) {
            EraseHolderReservationLocked(reservation_id);
          }
        }

        candidate_reports_sent_.erase(member_task_id);
        task_states_.erase(member_task_id);
        owner_retained_tasks_.erase(member_task_id);
      }

      const bool keep_terminal_marker =
          InitialFrontierPiggybackEnabledCached() && group->Full();
      candidate_reports_sent_.erase(task_id);
      recovery_frontier_protection_manifests_.erase(protection_it);
      RAY_CHECK(recovery_frontier_planner_->EraseGroup(task_id));
      if (keep_terminal_marker) {
        // A delayed owner-to-borrower recipe must not recreate this group.
        // Retain only the leader tombstone, not any member replay recipes.
        auto &terminal = task_states_[task_id];
        terminal.manifest.CopyFrom(tombstone);
        terminal.manifest_committed = true;
      }
      return true;
    }
  }

  const auto task_it = task_states_.find(task_id);

  if (task_it != task_states_.end() && !task_it->second.manifest.task_id().empty()) {
    const int comparison = CompareManifestVersions(tombstone, task_it->second.manifest);

    if (comparison < 0) {
      return false;
    }

    if (comparison == 0 &&
        tombstone.SerializeAsString() != task_it->second.manifest.SerializeAsString()) {
      return false;
    }
  }

  TaskRecoveryState &state = task_states_[task_id];
  state.manifest.CopyFrom(tombstone);
  state.manifest_committed = true;
  state.task_spec.reset();
  state.provisional_reservation_id.clear();
  state.provisional_piggyback_task_spec = false;

  EraseTaskObjectMetadataLocked(task_id);

  const auto reservation_it = holder_reservation_by_task_.find(task_id);
  if (reservation_it != holder_reservation_by_task_.end()) {
    std::vector<std::string> reservation_ids;
    reservation_ids.reserve(reservation_it->second.size());
    for (const auto &[rank, reservation_id] : reservation_it->second) {
      static_cast<void>(rank);
      reservation_ids.push_back(reservation_id);
    }
    for (const std::string &reservation_id : reservation_ids) {
      EraseHolderReservationLocked(reservation_id);
    }
  }

  candidate_reports_sent_.erase(task_id);

  return true;
}


void RecoverySuccessionManager::EraseTaskObjectMetadataLocked(
    const TaskID &task_id) {
  const auto object_ids_it = task_object_ids_.find(task_id);
  if (object_ids_it != task_object_ids_.end()) {
    for (const ObjectID &object_id : object_ids_it->second) {
      object_recovery_metadata_.erase(object_id);
      borrowed_objects_.erase(object_id);
    }
    task_object_ids_.erase(object_ids_it);
  }

  for (auto it = borrowed_objects_.begin(); it != borrowed_objects_.end();) {
    if (it->second.task_id == task_id) {
      object_recovery_metadata_.erase(it->first);
      const auto erase_it = it++;
      borrowed_objects_.erase(erase_it);
    } else {
      ++it;
    }
  }
}


void RecoverySuccessionManager::HandleWorkerFailure(const WorkerID &worker_id) {
  if (worker_id.IsNil()) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  failed_workers_.insert(worker_id);
}

void RecoverySuccessionManager::HandleNodeFailure(const NodeID &node_id) {
  if (node_id.IsNil()) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  failed_nodes_.insert(node_id);
}

bool RecoverySuccessionManager::IsRecoveryHolderKnownFailed(
    const rpc::RecoveryHolder &holder) const {
  const rpc::Address &address = holder.address();

  absl::MutexLock lock(&mutex_);

  if (address.worker_id().size() == WorkerID::Size() &&
      failed_workers_.contains(WorkerID::FromBinary(address.worker_id()))) {
    return true;
  }

  if (address.node_id().size() == NodeID::Size() &&
      failed_nodes_.contains(NodeID::FromBinary(address.node_id()))) {
    return true;
  }

  return false;
}

bool RecoverySuccessionManager::HasConfirmedHolderResponsibilities() const {
  absl::MutexLock lock(&mutex_);

  for (const auto &[task_id, task_state] : task_states_) {
    static_cast<void>(task_id);

    if (!task_state.manifest_committed || !task_state.task_spec.has_value()) {
      continue;
    }

    for (const rpc::RecoveryHolder &holder : task_state.manifest.succession()) {
      if (holder.rank() == 0) {
        continue;
      }

      if (SameWorker(holder.address(), self_address_)) {
        return true;
      }
    }
  }

  return false;
}

RecoverySuccessionManager::RecoverySuccessionProfile
RecoverySuccessionManager::GetProfileSnapshot() const {
  absl::MutexLock lock(&mutex_);
  auto snapshot = profile_;
  if (profiling_enabled_) {
    for (const auto &entry : task_states_) {
      if (entry.second.task_spec.IsShared()) {
        ++snapshot.shared_holder_recipes_current;
        snapshot.shared_holder_recipe_bytes_current +=
            entry.second.task_spec->ByteSizeLong();
      }
    }
  }
  return snapshot;
}

void RecoverySuccessionManager::ResetProfile() {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  profile_ = RecoverySuccessionProfile{};

  // Patch 4L gauges describe real retained state, not just events after reset.
  // Reconstruct them so a benchmark profile reset cannot make a later release
  // underflow or hide already-live owner lineage.
  profile_.owner_retained_task_specs_current =
      static_cast<uint64_t>(owner_retained_tasks_.size());

  for (const auto &entry : owner_retained_tasks_) {
    profile_.owner_retained_task_spec_bytes_current +=
        entry.second.task_spec_bytes;
  }

  profile_.owner_retained_task_specs_peak =
      profile_.owner_retained_task_specs_current;
  profile_.owner_retained_task_spec_bytes_peak =
      profile_.owner_retained_task_spec_bytes_current;
}

void RecoverySuccessionManager::RecordCandidateReport(bool accepted) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.candidate_reports_received;

  if (accepted) {
    ++profile_.candidate_reports_accepted;
  }
}

void RecoverySuccessionManager::RecordHolderInstallRpcSent(
    uint64_t task_spec_bytes,
    uint64_t manifest_bytes) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.holder_install_rpcs_sent;
  profile_.task_spec_bytes_sent += task_spec_bytes;
  profile_.manifest_bytes_sent += manifest_bytes;
}

void RecoverySuccessionManager::RecordHolderInstallRpcLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.holder_install_rpcs_completed;
  profile_.holder_install_rpc_time_ns += latency_ns;
}


void RecoverySuccessionManager::RecordFrontierRecipeEncoding(
    uint64_t elapsed_ns, uint64_t members, uint64_t bytes) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.frontier_recipe_encode_calls;
  profile_.frontier_recipe_encode_time_ns += elapsed_ns;
  profile_.frontier_recipe_encode_members += members;
  profile_.frontier_recipe_encode_bytes += bytes;
}

void RecoverySuccessionManager::RecordHolderInstallHandler(uint64_t elapsed_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.holder_install_handler_calls;
  profile_.holder_install_handler_time_ns += elapsed_ns;
}

void RecoverySuccessionManager::RecordHolderInstallCallback(uint64_t elapsed_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.holder_install_callback_calls;
  profile_.holder_install_callback_time_ns += elapsed_ns;
}

void RecoverySuccessionManager::RecordOwnerTaskSpecCopyLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.owner_task_spec_copy_count;
  profile_.owner_task_spec_copy_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordWitnessUpdateRpcSent(
    uint64_t task_spec_bytes,
    uint64_t manifest_bytes) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.witness_update_rpcs_sent;
  profile_.task_spec_bytes_sent += task_spec_bytes;
  profile_.manifest_bytes_sent += manifest_bytes;
}

void RecoverySuccessionManager::RecordWitnessUpdateRpcLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.witness_update_rpcs_completed;
  profile_.witness_update_rpc_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordWitnessUpdateRpcBreakdown(
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
    uint32_t batch_size) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  profile_.witness_update_client_queue_time_ns += client_queue_ns;
  profile_.witness_update_client_submit_to_cq_time_ns +=
      client_submit_to_cq_ns;
  profile_.witness_update_client_cq_to_main_loop_time_ns +=
      client_cq_to_main_loop_ns;
  profile_.witness_update_client_main_loop_to_batch_callback_time_ns +=
      client_main_loop_to_batch_callback_ns;
  if (client_submit_to_cq_ns != 0 || client_cq_to_main_loop_ns != 0 ||
      client_main_loop_to_batch_callback_ns != 0) {
    ++profile_.witness_update_client_phase_samples;
  }
  profile_.witness_update_client_enqueue_cpu_time_ns +=
      client_enqueue_cpu_ns;
  profile_.witness_update_client_batch_build_cpu_time_ns +=
      client_batch_build_cpu_ns;
  profile_.witness_update_client_batch_demux_cpu_time_ns +=
      client_batch_demux_cpu_ns;
  profile_.witness_update_server_batch_queue_time_ns += server_batch_queue_ns;
  profile_.witness_update_handler_time_ns += handler_ns;
  if (handler_ns != 0) {
    ++profile_.witness_update_handler_samples;
  }
  profile_.witness_update_mutex_wait_time_ns += mutex_wait_ns;
  profile_.witness_update_mutex_hold_time_ns += mutex_hold_ns;
  if (batch_leader) {
    ++profile_.witness_update_physical_batches_completed;
    profile_.witness_update_physical_batch_items += batch_size;
  }
}

void RecoverySuccessionManager::RecordHolderAdmissionPrepareCpu(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.holder_admission_prepare_cpu_calls;
  profile_.holder_admission_prepare_cpu_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordWitnessRequestBuildCpu(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.witness_request_build_cpu_calls;
  profile_.witness_request_build_cpu_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordWitnessLogicalCallbackCpu(
    uint64_t latency_ns, bool winner) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.witness_logical_callback_cpu_calls;
  profile_.witness_logical_callback_cpu_time_ns += latency_ns;
  if (winner) {
    ++profile_.witness_winner_callback_cpu_calls;
    profile_.witness_winner_callback_cpu_time_ns += latency_ns;
  } else {
    ++profile_.witness_redundant_callback_cpu_calls;
    profile_.witness_redundant_callback_cpu_time_ns += latency_ns;
  }
}

void RecoverySuccessionManager::RecordHolderCommitCpu(uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.holder_commit_cpu_calls;
  profile_.holder_commit_cpu_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordH2ReadinessAtH1Publish(
    bool h2_reserved, bool h2_installed) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.h1_publish_readiness_samples;
  if (h2_reserved) {
    ++profile_.h2_reserved_at_h1_publish;
  }
  if (h2_installed) {
    ++profile_.h2_installed_at_h1_publish;
  }
}

void RecoverySuccessionManager::RecordH2ReadinessAtH1Ack(
    bool h2_reserved, bool h2_installed) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.h1_ack_readiness_samples;
  if (h2_reserved) {
    ++profile_.h2_reserved_at_h1_ack;
  }
  if (h2_installed) {
    ++profile_.h2_installed_at_h1_ack;
  }
}


void RecoverySuccessionManager::RecordWitnessPublishLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.witness_publish_count;
  profile_.witness_publish_time_ns += latency_ns;

  if (latency_ns > profile_.witness_publish_max_time_ns) {
    profile_.witness_publish_max_time_ns = latency_ns;
  }
}




void RecoverySuccessionManager::RecordHolderCommitRpcSent(
    uint64_t manifest_bytes) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.holder_commit_rpcs_sent;
  profile_.manifest_bytes_sent += manifest_bytes;
}

void RecoverySuccessionManager::RecordHolderCommitRpcLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.holder_commit_rpcs_completed;
  profile_.holder_commit_rpc_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordHolderAdmissionLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  profile_.holder_admission_time_ns += latency_ns;

  if (latency_ns > profile_.holder_admission_max_time_ns) {
    profile_.holder_admission_max_time_ns = latency_ns;
  }
}


void RecoverySuccessionManager::RecordTaskArgumentMetadataLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.task_argument_metadata_calls;
  profile_.task_argument_metadata_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordInitialManifestBuild(
    uint64_t latency_ns,
    uint64_t manifest_bytes) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.initial_manifest_build_count;
  profile_.initial_manifest_build_time_ns += latency_ns;
  profile_.initial_manifest_bytes += manifest_bytes;
}

void RecoverySuccessionManager::RecordWitnessSelectionLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.witness_selection_count;
  profile_.witness_selection_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordWitnessGcsQueryLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.witness_gcs_query_count;
  profile_.witness_gcs_query_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordTaskSpecManifestAttachLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.task_spec_manifest_attach_count;
  profile_.task_spec_manifest_attach_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordRegisterOwnedTaskLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.register_owned_task_count;
  profile_.register_owned_task_time_ns += latency_ns;
}


void RecoverySuccessionManager::RecordEnsureTaskArgumentsLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.ensure_task_arguments_calls;
  profile_.ensure_task_arguments_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordCandidateQueueLatency(uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.candidate_queue_calls;
  profile_.candidate_queue_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordCandidateRpcSent(
    uint64_t logical_reports, uint64_t request_bytes) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  profile_.candidate_rpc_logical_reports_sent += logical_reports;
  ++profile_.candidate_rpc_physical_rpcs_sent;
  profile_.candidate_rpc_request_bytes_sent += request_bytes;
}

void RecoverySuccessionManager::RecordCandidateRpcLatency(
    uint64_t logical_reports, uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  profile_.candidate_rpc_logical_reports_completed += logical_reports;
  ++profile_.candidate_rpc_physical_rpcs_completed;
  profile_.candidate_rpc_time_ns += latency_ns;
}











}  // namespace ray::core