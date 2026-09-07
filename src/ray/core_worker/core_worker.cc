// Copyright 2017 The Ray Authors.
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

#include "ray/core_worker/core_worker.h"

#include <algorithm>
#include <future>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/core_worker/core_worker_shutdown_executor.h"
#include "ray/core_worker/recovery_succession_manager.h"
#include "ray/core_worker/shutdown_coordinator.h"

#ifndef _WIN32
#include <unistd.h>
#endif

#include <google/protobuf/util/json_util.h>

#include <cstdint>
#include <thread>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "ray/asio/periodical_runner.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/protobuf_utils.h"
#include "ray/common/ray_config.h"
#include "ray/common/runtime_env_common.h"
#include "ray/common/task/task_util.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"
#include "ray/rpc/event_aggregator_client.h"
#include "ray/util/container_util.h"
#include "ray/util/event.h"
#include "ray/util/process_utils.h"
#include "ray/util/subreaper.h"
#include <chrono>


using json = nlohmann::json;
using MessageType = ray::protocol::MessageType;

namespace ray::core {

// Patch 4D: pipelined holder admission.
// Patch 4E: batched recovery control RPCs.
// Patch 4E-1: first-holder candidate-report fast path.
// Patch 4F: first-holder TaskSpec piggyback.
// Patch 4G: hot-path profiling.
// Patch 4H: compact task-argument recovery metadata.
// Patch 4I: TaskSpec-level recovery argument sidecar.
// Patch 4J: task-centric recovery state.
// Patch 4K: batched H1 candidate/install path.
// Patch 4L: correctness-preserving retained owner TaskSpec for late borrow.

namespace {
// Default capacity for serialization caches.
constexpr size_t kDefaultSerializationCacheCap = 500;

// Patch 4E physical batching knobs. These alter only transport coalescing,
// never logical holder/witness semantics.
constexpr size_t kRecoveryCandidateBatchMaxItems = 64;
constexpr int64_t kRecoveryCandidateBatchDelayUs = 500;
constexpr size_t kRecoveryInstallBatchMaxItems = 64;
constexpr uint64_t kRecoveryInstallBatchMaxBytes = 4ULL * 1024ULL * 1024ULL;

// Implements setting the transient RUNNING_IN_RAY_GET and RUNNING_IN_RAY_WAIT states.
// These states override the RUNNING state of a task.
class ScopedTaskMetricSetter {
 public:
  ScopedTaskMetricSetter(const WorkerContext &ctx,
                         TaskCounter &ctr,
                         rpc::TaskStatus status)
      : status_(status), ctr_(ctr) {
    auto task_spec = ctx.GetCurrentTask();
    if (task_spec != nullptr) {
      task_name_ = task_spec->GetName();
      is_retry_ = task_spec->IsRetry();
    } else {
      task_name_ = "Unknown task";
      is_retry_ = false;
    }
    ctr_.SetMetricStatus(task_name_, status, is_retry_);
  }

  ScopedTaskMetricSetter(const ScopedTaskMetricSetter &) = delete;
  ScopedTaskMetricSetter &operator=(const ScopedTaskMetricSetter &) = delete;

  ~ScopedTaskMetricSetter() { ctr_.UnsetMetricStatus(task_name_, status_, is_retry_); }

 private:
  rpc::TaskStatus status_;
  TaskCounter &ctr_;
  std::string task_name_;
  bool is_retry_;
};

using ActorLifetime = ray::rpc::JobConfig_ActorLifetime;

// Helper function converts GetObjectLocationsOwnerReply to ObjectLocation
ObjectLocation CreateObjectLocation(
    const rpc::WorkerObjectLocationsPubMessage &object_info) {
  std::vector<NodeID> node_ids;
  node_ids.reserve(object_info.node_ids_size());
  for (int i = 0; i < object_info.node_ids_size(); ++i) {
    node_ids.push_back(NodeID::FromBinary(object_info.node_ids(i)));
  }
  bool is_spilled = !object_info.spilled_url().empty();
  // If the object size is unknown it's unset, and we use -1 to indicate that.
  int64_t object_size = object_info.object_size() == 0 ? -1 : object_info.object_size();
  return ObjectLocation(object_size,
                        std::move(node_ids),
                        is_spilled,
                        object_info.spilled_url(),
                        NodeID::FromBinary(object_info.spilled_node_id()),
                        object_info.did_spill());
}

std::optional<ObjectLocation> TryGetLocalObjectLocation(
    ReferenceCounterInterface &reference_counter, const ObjectID &object_id) {
  if (!reference_counter.HasReference(object_id)) {
    return std::nullopt;
  }
  rpc::WorkerObjectLocationsPubMessage object_info;
  reference_counter.FillObjectInformation(object_id, &object_info);
  // Note: there can be a TOCTOU race condition: HasReference returned true, but before
  // FillObjectInformation the object is released. Hence we check the ref_removed field.
  if (object_info.ref_removed()) {
    return std::nullopt;
  }
  return CreateObjectLocation(object_info);
}

/// Converts rpc::WorkerExitType to ShutdownReason
/// \param exit_type The worker exit type to convert
/// \param is_force_exit If true, INTENDED_USER_EXIT maps to kForcedExit; otherwise
/// kGracefulExit
ShutdownReason ConvertExitTypeToShutdownReason(rpc::WorkerExitType exit_type,
                                               bool is_force_exit = false) {
  switch (exit_type) {
  case rpc::WorkerExitType::INTENDED_SYSTEM_EXIT:
    return ShutdownReason::kIntentionalShutdown;
  case rpc::WorkerExitType::INTENDED_USER_EXIT:
    return is_force_exit ? ShutdownReason::kForcedExit : ShutdownReason::kGracefulExit;
  case rpc::WorkerExitType::USER_ERROR:
    return ShutdownReason::kUserError;
  case rpc::WorkerExitType::SYSTEM_ERROR:
    return ShutdownReason::kUnexpectedError;
  case rpc::WorkerExitType::NODE_OUT_OF_MEMORY:
    return ShutdownReason::kOutOfMemory;
  default:
    return ShutdownReason::kUnexpectedError;
  }
}

uint64_t StableWitnessScore(const TaskID &task_id, const NodeID &node_id) {
  // FNV-1a over TaskID || NodeID.
  constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
  constexpr uint64_t kPrime = 1099511628211ULL;

  uint64_t hash = kOffsetBasis;

  const std::string input = task_id.Binary() + node_id.Binary();

  for (const unsigned char byte : input) {
    hash ^= static_cast<uint64_t>(byte);
    hash *= kPrime;
  }

  return hash;
}

uint64_t StableWitnessScoreOptimized(const std::string &task_id_binary,
                                     const NodeID &node_id) {
  // Bit-for-bit identical FNV-1a input to StableWitnessScore, but the TaskID
  // binary is computed once per selection and no concatenated string is built.
  constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
  constexpr uint64_t kPrime = 1099511628211ULL;

  uint64_t hash = kOffsetBasis;
  for (const unsigned char byte : task_id_binary) {
    hash ^= static_cast<uint64_t>(byte);
    hash *= kPrime;
  }

  const std::string node_id_binary = node_id.Binary();
  for (const unsigned char byte : node_id_binary) {
    hash ^= static_cast<uint64_t>(byte);
    hash *= kPrime;
  }
  return hash;
}


uint64_t RecoveryProfileNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

int CompareRecoveryManifestVersions(const rpc::RecoveryManifest &left,
                                    const rpc::RecoveryManifest &right) {
  if (left.version().generation() < right.version().generation()) {
    return -1;
  }

  if (left.version().generation() > right.version().generation()) {
    return 1;
  }

  return 0;
}


// One publication retains its own threshold and outcome. The experiment only
// shares synchronization across results delivered together, never durability.
struct BatchedWitnessPublication {
  size_t witness_count = 0;
  bool require_all_witnesses = false;
  size_t completed = 0;
  size_t stored_count = 0;
  bool callback_sent = false;
  std::optional<rpc::RecoveryManifest> newest_manifest;
  std::function<void(bool, std::optional<rpc::RecoveryManifest>)> callback;
};

struct BatchedWitnessAckContext : RecoveryWitnessAckContext {
  std::shared_ptr<BatchedWitnessPublication> publication;
  uint64_t witness_start_ns = 0;
};

class OwnerWitnessAckBatchHandler : public RecoveryWitnessAckBatchHandler {
 public:
  explicit OwnerWitnessAckBatchHandler(
      std::shared_ptr<RecoverySuccessionManager> manager, bool profiling)
      : manager_(std::move(manager)), profiling_(profiling) {}

  void HandleReplies(const Status &status,
                     const std::vector<RecoveryWitnessAckResult> &results) override {
    if (results.empty()) {
      return;
    }
    struct Completion {
      bool ready = false;
      bool success = false;
      std::optional<rpc::RecoveryManifest> newest_manifest;
    };
    const uint64_t start_ns = profiling_ ? RecoveryProfileNowNs() : 0;
    std::vector<Completion> completions(results.size());
    uint64_t lock_wait_ns = 0;
    {
      const uint64_t wait_start_ns = profiling_ ? RecoveryProfileNowNs() : 0;
      absl::MutexLock lock(&mutex_);
      if (profiling_) {
        lock_wait_ns = RecoveryProfileNowNs() - wait_start_ns;
      }
      for (size_t i = 0; i < results.size(); ++i) {
        const auto &context =
            static_cast<const BatchedWitnessAckContext &>(*results[i].context);
        auto &state = *context.publication;
        const auto &reply = *results[i].reply;
        ++state.completed;
        const bool stored = status.ok() && reply.stored();
        if (stored) {
          ++state.stored_count;
        }
        if (reply.has_latest_manifest() &&
            (!state.newest_manifest.has_value() ||
             CompareRecoveryManifestVersions(reply.latest_manifest(),
                                             *state.newest_manifest) > 0)) {
          state.newest_manifest = reply.latest_manifest();
        }
        if (state.callback_sent) {
          continue;
        }
        auto &completion = completions[i];
        if (state.require_all_witnesses) {
          completion.ready = state.completed == state.witness_count;
          completion.success = state.stored_count == state.witness_count;
        } else {
          completion.ready = stored || state.completed == state.witness_count;
          completion.success = stored;
        }
        if (completion.ready) {
          state.callback_sent = true;
          if (!completion.success) {
            completion.newest_manifest = state.newest_manifest;
          }
        }
      }
    }
    const uint64_t bookkeeping_ns = profiling_ ? RecoveryProfileNowNs() - start_ns : 0;
    if (profiling_) {
      batches_.fetch_add(1, std::memory_order_relaxed);
      items_.fetch_add(results.size(), std::memory_order_relaxed);
      bookkeeping_time_ns_.fetch_add(bookkeeping_ns, std::memory_order_relaxed);
      lock_wait_time_ns_.fetch_add(lock_wait_ns, std::memory_order_relaxed);
    }

    // No admission, rollback, or reentrant enqueue runs under the batch mutex.
    // Each publication still executes its existing continuation exactly once.
    for (size_t i = 0; i < results.size(); ++i) {
      const auto &context =
          static_cast<const BatchedWitnessAckContext &>(*results[i].context);
      const auto &reply = *results[i].reply;
      if (context.witness_start_ns != 0) {
        manager_->RecordWitnessUpdateRpcLatency(
            RecoveryProfileNowNs() - context.witness_start_ns);
        manager_->RecordWitnessUpdateRpcBreakdown(
            reply.client_queue_time_ns(),
            reply.client_submit_to_cq_time_ns(),
            reply.client_cq_to_main_loop_time_ns(),
            reply.client_main_loop_to_batch_callback_time_ns(),
            reply.client_enqueue_cpu_time_ns(),
            reply.client_batch_build_cpu_time_ns(),
            reply.client_batch_demux_cpu_time_ns(),
            reply.witness_batch_queue_time_ns(),
            reply.witness_handler_time_ns(),
            reply.witness_mutex_wait_time_ns(),
            reply.witness_mutex_hold_time_ns(),
            reply.client_batch_leader(),
            reply.client_batch_size());
      }
      auto &completion = completions[i];
      const uint64_t continuation_start_ns = profiling_ ? RecoveryProfileNowNs() : 0;
      if (completion.ready) {
        context.publication->callback(completion.success,
                                      std::move(completion.newest_manifest));
      }
      if (profiling_) {
        manager_->RecordWitnessLogicalCallbackCpu(
            bookkeeping_ns / results.size() +
                RecoveryProfileNowNs() - continuation_start_ns,
            completion.ready);
      }
    }
  }

  RecoveryWitnessAckBatchStats GetStats() const override {
    return {batches_.load(std::memory_order_relaxed),
            items_.load(std::memory_order_relaxed),
            bookkeeping_time_ns_.load(std::memory_order_relaxed),
            lock_wait_time_ns_.load(std::memory_order_relaxed)};
  }

  void ResetStats() override {
    batches_.store(0, std::memory_order_relaxed);
    items_.store(0, std::memory_order_relaxed);
    bookkeeping_time_ns_.store(0, std::memory_order_relaxed);
    lock_wait_time_ns_.store(0, std::memory_order_relaxed);
  }

 private:
  // All publication counters above are protected by this owner-wide mutex.
  // The profiler reports waiting separately to expose cross-witness contention.
  absl::Mutex mutex_;
  const std::shared_ptr<RecoverySuccessionManager> manager_;
  const bool profiling_;
  std::atomic<uint64_t> batches_{0};
  std::atomic<uint64_t> items_{0};
  std::atomic<uint64_t> bookkeeping_time_ns_{0};
  std::atomic<uint64_t> lock_wait_time_ns_{0};
};

bool MergeRecoveryWitnessViews(const rpc::RecoveryManifest &incoming,
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

  if (incoming.tombstoned()) {
    if (incoming.version().generation() >= state->version().generation()) {
      state->CopyFrom(incoming);
    }
    return true;
  }
  if (state->tombstoned()) {
    return true;
  }

  rpc::RecoveryHolder owner;
  bool have_owner = false;
  std::vector<rpc::RecoveryHolder> holders;

  auto absorb = [&](const rpc::RecoveryManifest &manifest) {
    for (const rpc::RecoveryHolder &holder : manifest.succession()) {
      if (holder.rank() == 0) {
        if (!have_owner) {
          owner.CopyFrom(holder);
          have_owner = true;
        }
        continue;
      }
      bool duplicate = false;
      for (const rpc::RecoveryHolder &existing : holders) {
        if (!existing.address().worker_id().empty() &&
            existing.address().worker_id() == holder.address().worker_id()) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        holders.push_back(holder);
      }
    }
  };

  absorb(*state);
  absorb(incoming);
  if (!have_owner ||
      holders.size() > static_cast<size_t>(state->target_holder_count())) {
    return false;
  }

  std::sort(holders.begin(), holders.end(),
            [](const rpc::RecoveryHolder &a, const rpc::RecoveryHolder &b) {
              return a.address().worker_id() < b.address().worker_id();
            });

  state->clear_succession();
  rpc::RecoveryHolder *out_owner = state->add_succession();
  out_owner->CopyFrom(owner);
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

}  // namespace

JobID GetProcessJobID(const CoreWorkerOptions &options) {
  if (options.worker_type == WorkerType::DRIVER) {
    RAY_CHECK(!options.job_id.IsNil());
  } else {
    RAY_CHECK(options.job_id.IsNil());
  }

  if (options.worker_type == WorkerType::WORKER) {
    // For workers, the job ID is assigned by Raylet via an environment variable.
    const std::string &job_id_env = RayConfig::instance().JOB_ID();
    RAY_CHECK(!job_id_env.empty());
    return JobID::FromHex(job_id_env);
  }
  return options.job_id;
}

TaskCounter::TaskCounter(ray::observability::MetricInterface &task_by_state_gauge,
                         ray::observability::MetricInterface &actor_by_state_gauge)
    : task_by_state_gauge_(task_by_state_gauge),
      actor_by_state_gauge_(actor_by_state_gauge) {
  // On change we only retract keys that just dropped to zero (emit their final 0).
  // Live keys are re-asserted every tick by the ForEachEntry loop in RecordMetrics,
  // so emitting them here too would double-record. This split keeps each key
  // recorded exactly once per tick.
  counter_.SetOnChangeCallback(
      [this](const std::tuple<std::string, TaskStatusType, bool> &key)
          ABSL_EXCLUSIVE_LOCKS_REQUIRED(&mu_) mutable {
            if (counter_.Get(key) == 0) {
              RecordRunningTaskBreakdown(key, /*running_total=*/0);
            }
          });
}

void TaskCounter::RecordRunningTaskBreakdown(
    const std::tuple<std::string, TaskStatusType, bool> &key, int64_t running_total) {
  if (std::get<1>(key) != TaskStatusType::kRunning) {
    return;
  }
  const auto &func_name = std::get<0>(key);
  const auto is_retry = std::get<2>(key);
  const int64_t num_in_get = running_in_get_counter_.Get({func_name, is_retry});
  const int64_t num_in_wait = running_in_wait_counter_.Get({func_name, is_retry});
  const int64_t num_getting_pinning_args =
      pending_getting_and_pinning_args_fetch_counter_.Get({func_name, is_retry});
  const auto is_retry_label = is_retry ? "1" : "0";
  // RUNNING_IN_RAY_GET/WAIT are sub-states of RUNNING, so we need to subtract
  // them out to avoid double-counting.
  task_by_state_gauge_.Record(
      running_total - num_in_get - num_in_wait - num_getting_pinning_args,
      {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING)},
       {"Name", func_name},
       {"IsRetry", is_retry_label},
       {"JobId", job_id_},
       {"Source", "executor"}});
  // Negate the metrics recorded from the submitter process for these tasks.
  task_by_state_gauge_.Record(
      -running_total,
      {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::SUBMITTED_TO_WORKER)},
       {"Name", func_name},
       {"IsRetry", is_retry_label},
       {"JobId", job_id_},
       {"Source", "executor"}});
  // Record sub-state for get.
  task_by_state_gauge_.Record(
      num_in_get,
      {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING_IN_RAY_GET)},
       {"Name", func_name},
       {"IsRetry", is_retry_label},
       {"JobId", job_id_},
       {"Source", "executor"}});
  // Record sub-state for wait.
  task_by_state_gauge_.Record(
      num_in_wait,
      {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING_IN_RAY_WAIT)},
       {"Name", func_name},
       {"IsRetry", is_retry_label},
       {"JobId", job_id_},
       {"Source", "executor"}});
  // Record sub-state for pending args fetch.
  task_by_state_gauge_.Record(
      num_getting_pinning_args,
      {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::GETTING_AND_PINNING_ARGS)},
       {"Name", func_name},
       {"IsRetry", is_retry_label},
       {"JobId", job_id_},
       {"Source", "executor"}});
}

void TaskCounter::RecordMetrics() {
  absl::MutexLock l(&mu_);
  // Re-assert every live RUNNING-state entry each tick, not just transitions: the
  // metrics backend clears gauge observations after each export (#56405), so a
  // long-running task that hasn't transitioned would otherwise drop out of the
  // gauge. FlushOnChangeCallbacks still emits the final 0 for keys that just
  // dropped to zero (erased from the counter, so ForEachEntry won't visit them).
  counter_.FlushOnChangeCallbacks();
  counter_.ForEachEntry([this](const std::tuple<std::string, TaskStatusType, bool> &key,
                               int64_t running_total)
                            ABSL_EXCLUSIVE_LOCKS_REQUIRED(&mu_) {
                              RecordRunningTaskBreakdown(key, running_total);
                            });
  if (IsActor()) {
    float running_tasks = 0.0;
    float idle = 0.0;
    if (num_tasks_running_ == 0) {
      idle = 1.0;
    } else {
      running_tasks = 1.0;
    }
    actor_by_state_gauge_.Record(idle,
                                 {{"State", "ALIVE_IDLE"},
                                  {"Name", actor_name_},
                                  {"Source", "executor"},
                                  {"JobId", job_id_}});
    actor_by_state_gauge_.Record(running_tasks,
                                 {{"State", "ALIVE_RUNNING_TASKS"},
                                  {"Name", actor_name_},
                                  {"Source", "executor"},
                                  {"JobId", job_id_}});
  }
}

void TaskCounter::SetMetricStatus(const std::string &func_name,
                                  rpc::TaskStatus status,
                                  bool is_retry) {
  absl::MutexLock l(&mu_);
  // Add a no-op increment to counter_ so that
  // it will invoke a callback upon RecordMetrics.
  counter_.Increment({func_name, TaskStatusType::kRunning, is_retry}, 0);
  if (status == rpc::TaskStatus::RUNNING_IN_RAY_GET) {
    running_in_get_counter_.Increment({func_name, is_retry});
  } else if (status == rpc::TaskStatus::RUNNING_IN_RAY_WAIT) {
    running_in_wait_counter_.Increment({func_name, is_retry});
  } else if (status == rpc::TaskStatus::GETTING_AND_PINNING_ARGS) {
    pending_getting_and_pinning_args_fetch_counter_.Increment({func_name, is_retry});
  } else {
    RAY_CHECK(false) << "Unexpected status " << rpc::TaskStatus_Name(status);
  }
}

void TaskCounter::UnsetMetricStatus(const std::string &func_name,
                                    rpc::TaskStatus status,
                                    bool is_retry) {
  absl::MutexLock l(&mu_);
  // Add a no-op decrement to counter_ so that
  // it will invoke a callback upon RecordMetrics.
  counter_.Decrement({func_name, TaskStatusType::kRunning, is_retry}, 0);
  if (status == rpc::TaskStatus::RUNNING_IN_RAY_GET) {
    running_in_get_counter_.Decrement({func_name, is_retry});
  } else if (status == rpc::TaskStatus::RUNNING_IN_RAY_WAIT) {
    running_in_wait_counter_.Decrement({func_name, is_retry});
  } else if (status == rpc::TaskStatus::GETTING_AND_PINNING_ARGS) {
    pending_getting_and_pinning_args_fetch_counter_.Decrement({func_name, is_retry});
  } else {
    RAY_LOG(FATAL) << "Unexpected status " << rpc::TaskStatus_Name(status);
  }
}

CoreWorker::CoreWorker(
    CoreWorkerOptions options,
    std::unique_ptr<WorkerContext> worker_context,
    instrumented_io_context &io_service,
    instrumented_io_context &object_freed_callback_service,
    std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
    std::shared_ptr<rpc::RayletClientPool> raylet_client_pool,
    std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
    std::unique_ptr<rpc::GrpcServer> core_worker_server,
    rpc::Address rpc_address,
    std::shared_ptr<gcs::GcsClient> gcs_client,
    std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client,
    std::shared_ptr<RayletClientInterface> local_raylet_rpc_client,
    boost::thread &io_thread,
    boost::thread &object_freed_callback_thread,
    std::shared_ptr<ReferenceCounterInterface> reference_counter,
    std::shared_ptr<CoreWorkerMemoryStore> memory_store,
    std::shared_ptr<CoreWorkerPlasmaStoreProvider> plasma_store_provider,
    std::shared_ptr<experimental::MutableObjectProviderInterface>
        experimental_mutable_object_provider,
    std::unique_ptr<FutureResolver> future_resolver,
    std::shared_ptr<TaskManager> task_manager,
    std::shared_ptr<ActorCreatorInterface> actor_creator,
    std::unique_ptr<ActorTaskSubmitter> actor_task_submitter,
    std::unique_ptr<pubsub::PublisherInterface> object_info_publisher,
    std::unique_ptr<pubsub::SubscriberInterface> object_info_subscriber,
    std::shared_ptr<LeaseRequestRateLimiter> lease_request_rate_limiter,
    std::unique_ptr<NormalTaskSubmitter> normal_task_submitter,
    std::unique_ptr<ObjectRecoveryManager> object_recovery_manager,
    std::unique_ptr<ActorManager> actor_manager,
    instrumented_io_context &task_execution_service,
    std::unique_ptr<worker::TaskEventBuffer> task_event_buffer,
    uint32_t pid,
    ray::observability::MetricInterface &task_by_state_gauge,
    ray::observability::MetricInterface &actor_by_state_gauge,
    ClockInterface &clock)
    : options_(std::move(options)),
      get_call_site_(RayConfig::instance().record_ref_creation_sites()
                         ? options_.get_lang_stack
                         : nullptr),
      worker_context_(std::move(worker_context)),
      io_service_(io_service),
      object_freed_callback_service_(object_freed_callback_service),
      core_worker_client_pool_(std::move(core_worker_client_pool)),
      raylet_client_pool_(std::move(raylet_client_pool)),
      periodical_runner_(std::move(periodical_runner)),
      core_worker_server_(std::move(core_worker_server)),
      rpc_address_(std::move(rpc_address)),
      gcs_client_(std::move(gcs_client)),
      raylet_ipc_client_(std::move(raylet_ipc_client)),
      local_raylet_rpc_client_(std::move(local_raylet_rpc_client)),
      io_thread_(io_thread),
      object_freed_callback_thread_(object_freed_callback_thread),
      reference_counter_(std::move(reference_counter)),
      memory_store_(std::move(memory_store)),
      plasma_store_provider_(std::move(plasma_store_provider)),
      experimental_mutable_object_provider_(
          std::move(experimental_mutable_object_provider)),
      future_resolver_(std::move(future_resolver)),
      task_manager_(std::move(task_manager)),
      actor_creator_(std::move(actor_creator)),
      actor_task_submitter_(std::move(actor_task_submitter)),
      object_info_publisher_(std::move(object_info_publisher)),
      object_info_subscriber_(std::move(object_info_subscriber)),
      lease_request_rate_limiter_(std::move(lease_request_rate_limiter)),
      normal_task_submitter_(std::move(normal_task_submitter)),
      recovery_succession_enabled_(
          RayConfig::instance().enable_recovery_succession()),
      recovery_witness_holder_baseline_enabled_(
          recovery_succession_enabled_ &&
          RayConfig::instance().enable_recovery_witness_holder_baseline()),
      recovery_succession_profiling_enabled_(
          recovery_succession_enabled_ &&
          RayConfig::instance().enable_recovery_succession_profiling()),
      normal_submit_stage_profiling_enabled_(
          RayConfig::instance().enable_recovery_succession_profiling()),
      recovery_succession_manager_(nullptr),
      object_recovery_manager_(std::move(object_recovery_manager)),
      actor_manager_(std::move(actor_manager)),
      actor_id_(ActorID::Nil()),
      task_queue_length_(0),
      num_executed_tasks_(0),
      num_get_pin_args_in_flight_(0),
      num_failed_get_pin_args_(0),
      task_execution_service_(task_execution_service),
      exiting_detail_(std::nullopt),
      max_direct_call_object_size_(RayConfig::instance().max_direct_call_object_size()),
      task_counter_(task_by_state_gauge, actor_by_state_gauge),
      task_event_buffer_(std::move(task_event_buffer)),
      pid_(pid),
      actor_shutdown_callback_(options_.actor_shutdown_callback),
      runtime_env_json_serialization_cache_(kDefaultSerializationCacheCap),
      free_actor_object_callback_(
          [this, free_actor_object_callback = options_.free_actor_object_callback](
              const ObjectID &object_id) {
            // Need to post to the io service to prevent deadlock because this submits a
            // task and therefore needs to acquire the reference counter lock.
            io_service_.post([free_actor_object_callback,
                              object_id]() { free_actor_object_callback(object_id); },
                             "CoreWorker.FreeActorObjectCallback");
          }),
      clock_(clock) {
  if (recovery_succession_enabled_) {
    recovery_succession_manager_ =
        std::make_shared<RecoverySuccessionManager>(rpc_address_);

    if (RayConfig::instance().enable_recovery_witness_batch_ack()) {
      recovery_witness_ack_batch_handler_ = std::make_shared<OwnerWitnessAckBatchHandler>(
          recovery_succession_manager_, recovery_succession_profiling_enabled_);
    }

    task_manager_->SetLineageReleasedCallback([this](const TaskID &task_id) {
      // RemoveLineageReference holds the TaskManager lock.
      // Post the recovery work to the CoreWorker event loop.
      io_service_.post(
          [this, task_id] {
            if (!recovery_succession_enabled_ ||
                recovery_succession_manager_ == nullptr) {
              return;
            }

            // Adaptive Succession reuses TaskManager's native
            // reconstructable_return_ids_ lifetime. This callback fires only
            // after the language frontend and all dependent tasks release every
            // reconstructable return, so no second owner-return tracker is needed.
            if (!recovery_witness_holder_baseline_enabled_) {
              const bool should_tombstone =
                  recovery_succession_manager_->HandleOwnerTaskLineageReleased(
                      task_id);
              task_manager_->ReleaseTaskForRecoverySuccession(task_id);
              if (!should_tombstone) {
                return;
              }
            } else if (
                recovery_succession_manager_->OwnerTaskHasLiveReturns(task_id)) {
              // Fixed-R deliberately keeps its existing exact ObjectID callback
              // lifetime path for an uncontaminated baseline comparison.
              return;
            }

            auto tombstone = recovery_succession_manager_->BuildTombstoneForTask(task_id);

            if (!tombstone.has_value()) {
              return;
            }

            const TaskID tombstone_task_id =
                TaskID::FromBinary(tombstone->task_id());
            if (!recovery_tombstones_in_flight_.insert(tombstone_task_id).second) {
              return;
            }

            RAY_LOG(INFO).WithField(tombstone_task_id)
                << "Task lineage released; publishing recovery tombstone";

            PublishRecoveryTombstone(std::move(tombstone.value()));
          },
          "CoreWorker.PublishRecoveryTombstone");
    });

    if (future_resolver_ != nullptr) {
      std::weak_ptr<RecoverySuccessionManager> weak_recovery_manager =
          recovery_succession_manager_;

      future_resolver_->SetRecoveryMetadataCallback(
          [weak_recovery_manager](const ObjectID &object_id,
                                  const rpc::RecoveryObjectMetadata &metadata) {
            const auto recovery_manager = weak_recovery_manager.lock();

            if (recovery_manager == nullptr) {
              return;
            }

            recovery_manager->RegisterBorrowedObject(object_id, metadata);
          });
    }
  }

  // Initialize task receivers.
  if (options_.worker_type == WorkerType::WORKER) {
    RAY_CHECK(options_.task_execution_callback != nullptr);
    auto execute_task = std::bind(&CoreWorker::ExecuteTask,
                                  this,
                                  std::placeholders::_1,
                                  std::placeholders::_2,
                                  std::placeholders::_3,
                                  std::placeholders::_4,
                                  std::placeholders::_5,
                                  std::placeholders::_6,
                                  std::placeholders::_7,
                                  std::placeholders::_8,
                                  std::placeholders::_9);
    actor_task_execution_arg_waiter_ = std::make_unique<ActorTaskExecutionArgWaiter>(
        [this](const std::vector<rpc::ObjectReference> &args,
               const TaskID &task_id,
               int32_t attempt_number) {
          RAY_CHECK_OK(
              raylet_ipc_client_->WaitForActorCallArgs(args, task_id, attempt_number))
              << "WaitForActorCallArgs IPC failed unexpectedly";
        });
    task_receiver_ = std::make_unique<TaskReceiver>(task_execution_service_,
                                                    *task_event_buffer_,
                                                    execute_task,
                                                    *actor_task_execution_arg_waiter_,
                                                    options_.initialize_thread_callback);
  }

  RegisterToGcs(options_.worker_launch_time_ms, options_.worker_launched_time_ms);

  if (options_.worker_type == WorkerType::DRIVER || recovery_succession_enabled_) {
    SubscribeToNodeChanges();
  }

  // Create an entry for the driver task in the task table. This task is
  // added immediately with status RUNNING. This allows us to push errors
  // related to this driver task back to the driver. For example, if the
  // driver creates an object that is later evicted, we should notify the
  // user that we're unable to reconstruct the object, since we cannot
  // rerun the driver.
  if (options_.worker_type == WorkerType::DRIVER) {
    TaskSpecBuilder builder;
    const TaskID task_id = TaskID::ForDriverTask(worker_context_->GetCurrentJobID());
    builder.SetDriverTaskSpec(task_id,
                              options_.language,
                              worker_context_->GetCurrentJobID(),
                              // Driver has no parent task
                              /*parent_task_id=*/TaskID::Nil(),
                              GetCallerId(),
                              rpc_address_,
                              TaskID::Nil());
    // Drivers are never re-executed.
    SetCurrentTaskId(task_id, /*attempt_number=*/0, "driver");

    // Add the driver task info.
    if (task_event_buffer_->Enabled() &&
        !RayConfig::instance().task_events_skip_driver_for_test()) {
      auto spec = std::move(builder).ConsumeAndBuild();
      auto job_id = spec.JobId();
      auto task_event = std::make_unique<worker::TaskStatusEvent>(
          task_id,
          std::move(job_id),
          /*attempt_number=*/0,
          rpc::TaskStatus::RUNNING,
          /*timestamp=*/clock_.NowUnixNanos(),
          /*is_actor_task_event=*/false,
          options_.session_name,
          GetCurrentNodeId(),
          std::make_shared<const TaskSpecification>(std::move(spec)));
      task_event_buffer_->AddTaskEvent(std::move(task_event));
    }
  }

  if (options_.worker_type != WorkerType::DRIVER) {
    periodical_runner_->RunFnPeriodically(
        [this] { ExitIfParentRayletDies(); },
        RayConfig::instance().raylet_death_check_interval_milliseconds(),
        "CoreWorker.ExitIfParentRayletDies");
  }

  /// If periodic asio stats print is enabled, it will print it.
  const auto event_stats_print_interval_ms =
      RayConfig::instance().event_stats_print_interval_ms();
  if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
    periodical_runner_->RunFnPeriodically(
        [this] {
          RAY_LOG(INFO) << "Event stats:\n\n"
                        << io_service_.stats()->StatsString() << "\n\n"
                        << "-----------------\n"
                        << "Task execution event stats:\n"
                        << task_execution_service_.stats()->StatsString() << "\n\n"
                        << "-----------------\n"
                        << "Task Event stats:\n"
                        << task_event_buffer_->DebugString() << "\n";
        },
        event_stats_print_interval_ms,
        "CoreWorker.PrintEventStats");
  }

  periodical_runner_->RunFnPeriodically(
      [this] {
        const auto lost_objects = reference_counter_->FlushObjectsToRecover();
        if (!lost_objects.empty()) {
          // Keep :info_message: in sync with LOG_PREFIX_INFO_MESSAGE in ray_constants.py.
          RAY_LOG(ERROR) << ":info_message: Attempting to recover " << lost_objects.size()
                         << " lost objects by resubmitting their tasks or setting a new "
                            "primary location from existing copies. To disable object "
                            "reconstruction, set @ray.remote(max_retries=0).";
          // Delete the objects from the in-memory store to indicate that they are not
          // available. The object recovery manager will guarantee that a new value
          // will eventually be stored for the objects (either an
          // UnreconstructableError or a value reconstructed from lineage).
          memory_store_->Delete(lost_objects);
          for (const auto &object_id : lost_objects) {
            // NOTE(swang): There is a race condition where this can return false if
            // the reference went out of scope since the call to the ref counter to get
            // the lost objects. It's okay to not mark the object as failed or recover
            // the object since there are no reference holders.
            if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {
              RecoverySuccessionManager::BorrowedObjectRecoveryPlan plan;

              if (recovery_succession_manager_->GetBorrowedObjectRecoveryPlan(object_id,
                                                                              &plan)) {
                RecoverBorrowedObject(object_id, [this, object_id](bool started) {
                  if (!started) {
                    RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
                  }
                });

                continue;
              }
            }

            RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
          }
        }
      },
      100,
      "CoreWorker.RecoverObjects");

  periodical_runner_->RunFnPeriodically(
      [this] { InternalHeartbeat(); },
      RayConfig::instance().core_worker_internal_heartbeat_ms(),
      "CoreWorker.InternalHeartbeat");

  periodical_runner_->RunFnPeriodically(
      [this] {
        // Periodically report the backlog so that the local raylet can report the
        // resources needed for tasks that haven't had their dependencies resolved yet to
        // the GCS + Autoscaler.
        normal_task_submitter_->ReportWorkerBacklog();
      },
      RayConfig::instance().report_worker_backlog_interval_ms(),
      "CoreWorker.ReportWorkerBacklog");

  periodical_runner_->RunFnPeriodically(
      [this] { RecordMetrics(); },
      RayConfig::instance().metrics_report_interval_ms() / 2,
      "CoreWorker.RecordMetrics");

  periodical_runner_->RunFnPeriodically(
      [this] { TryDelPendingObjectRefStreams(); },
      RayConfig::instance().local_gc_min_interval_s() * 1000,
      "CoreWorker.TryDelPendingObjectRefStreams");

#ifndef _WIN32
  // Doing this last during CoreWorker initialization, so initialization logic like
  // registering with Raylet can finish with higher priority.
  static const bool niced = [this]() {
    if (options_.worker_type != WorkerType::DRIVER) {
      const auto niceness = nice(RayConfig::instance().worker_niceness());
      RAY_LOG(INFO) << "Adjusted worker niceness to " << niceness;
      return true;
    }
    return false;
  }();
  // Verify driver and worker are never mixed in the same process.
  RAY_CHECK_EQ(options_.worker_type != WorkerType::DRIVER, niced);
#endif
  // Tell the raylet the port that we are listening on.
  // NOTE: This also marks the worker as available in Raylet. We do this at the very end
  // in case there is a problem during construction.
  ConnectToRayletInternal();
}

CoreWorker::~CoreWorker() {
  WaitForShutdownComplete();
  RAY_LOG(INFO) << "Core worker is destructed";
}

void CoreWorker::InitializeShutdownExecutor() {
  auto executor = std::make_unique<CoreWorkerShutdownExecutor>(shared_from_this());
  shutdown_coordinator_ =
      std::make_unique<ShutdownCoordinator>(std::move(executor), options_.worker_type);

  RAY_LOG(DEBUG) << "Initialized unified shutdown coordinator with concrete executor for "
                    "worker type: "
                 << WorkerTypeString(options_.worker_type);
}

void CoreWorker::Shutdown() {
  shutdown_coordinator_->RequestShutdown(
      /*force_shutdown=*/false, ShutdownReason::kGracefulExit, "ray.shutdown() called");
}

void CoreWorker::WaitForShutdownComplete(std::chrono::milliseconds timeout_ms) {
  if (shutdown_coordinator_ && shutdown_coordinator_->IsShuttingDown()) {
    shutdown_coordinator_->GetExecutor()->WaitForCompletion(timeout_ms);
  }
}

void CoreWorker::ConnectToRayletInternal() {
  // Tell the raylet the port that we are listening on.
  // NOTE: This also marks the worker as available in Raylet. We do this at the
  // very end in case there is a problem during construction.
  if (options_.worker_type == WorkerType::DRIVER) {
    Status status = raylet_ipc_client_->AnnounceWorkerPortForDriver(
        core_worker_server_->GetPort(), options_.entrypoint);
    RAY_CHECK_OK(status) << "Failed to announce driver's port to raylet and GCS";
  } else {
    Status status =
        raylet_ipc_client_->AnnounceWorkerPortForWorker(core_worker_server_->GetPort());
    RAY_CHECK_OK(status) << "Failed to announce worker's port to raylet and GCS";
  }
}

void CoreWorker::Disconnect(
    const rpc::WorkerExitType &exit_type,
    const std::string &exit_detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  // Force stats export before exiting the worker.
  RecordMetrics();

  // Driver exiting.
  if (options_.worker_type == WorkerType::DRIVER && task_event_buffer_->Enabled() &&
      !RayConfig::instance().task_events_skip_driver_for_test()) {
    auto task_event = std::make_unique<worker::TaskStatusEvent>(
        worker_context_->GetCurrentTaskID(),
        worker_context_->GetCurrentJobID(),
        /*attempt_number=*/0,
        rpc::TaskStatus::FINISHED,
        /*timestamp=*/clock_.NowUnixNanos(),
        /*is_actor_task_event=*/worker_context_->GetCurrentActorID().IsNil(),
        options_.session_name,
        GetCurrentNodeId());
    task_event_buffer_->AddTaskEvent(std::move(task_event));
  }

  opencensus::stats::StatsExporter::ExportNow();

  if (connected_.exchange(false)) {
    RAY_LOG(INFO) << "Sending disconnect message to the local raylet.";
    Status status = raylet_ipc_client_->Disconnect(
        exit_type, exit_detail, creation_task_exception_pb_bytes);
    if (status.ok()) {
      RAY_LOG(INFO) << "Disconnected from the local raylet.";
    } else {
      RAY_LOG(WARNING) << "Failed to disconnect from the local raylet: " << status;
    }
  } else {
    RAY_LOG(DEBUG) << "Already disconnected, skipping disconnect message";
  }
}

void CoreWorker::KillChildProcs() {
  // There are cases where worker processes can "leak" child processes.
  // Basically this means that the worker process (either itself, or via
  // code in a task or actor) spawned a process and did not kill it on termination.
  // The process will continue living beyond the lifetime of the worker process.
  // If that leaked process has expensive resources, such as a CUDA context and associated
  // GPU memory, then those resources will never be cleaned until something else kills the
  // process.
  //
  // This function lists all processes that are direct children of the current worker
  // process, then kills them. This currently only works for the "happy-path"; worker
  // process crashes will still leak processes.
  // TODO(cade) Use more robust method to catch leaked processes even in worker crash
  // scenarios (subreaper).

  if (!RayConfig::instance().kill_child_processes_on_worker_exit()) {
    RAY_LOG(DEBUG)
        << "kill_child_processes_on_worker_exit is not true, skipping KillChildProcs";
    return;
  }

  RAY_LOG(DEBUG) << "kill_child_processes_on_worker_exit true, KillChildProcs";
  auto maybe_child_procs = GetAllProcsWithPpid(GetPID());

  // Enumerating child procs is not supported on this platform.
  if (!maybe_child_procs) {
    RAY_LOG(DEBUG) << "Killing leaked procs not supported on this platform.";
    return;
  }

  const auto &child_procs = *maybe_child_procs;
  const auto child_procs_str = absl::StrJoin(child_procs, ",");
  RAY_LOG(INFO) << "Try killing all child processes of this worker as it exits. "
                << "Child process pids: " << child_procs_str;

  for (const auto &child_pid : child_procs) {
    auto maybe_error_code = KillProc(child_pid);
    RAY_CHECK(maybe_error_code)
        << "Expected this path to only be called when KillProc is supported.";
    auto error_code = *maybe_error_code;

    RAY_LOG(INFO) << "Kill result for child pid " << child_pid << ": "
                  << error_code.message() << ", bool " << static_cast<bool>(error_code);
    if (error_code) {
      RAY_LOG(WARNING) << "Unable to kill potentially leaked process " << child_pid
                       << ": " << error_code.message();
    }
  }
}

void CoreWorker::Exit(
    const rpc::WorkerExitType exit_type,
    const std::string &detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  // Preserve actor creation failure details by marking a distinct shutdown reason
  // when initialization raised an exception. An exception payload is provided.
  ShutdownReason reason = creation_task_exception_pb_bytes != nullptr
                              ? ShutdownReason::kActorCreationFailed
                              : ConvertExitTypeToShutdownReason(exit_type);

  shutdown_coordinator_->RequestShutdown(/*force_shutdown=*/false,
                                         reason,
                                         detail,
                                         ShutdownCoordinator::kInfiniteTimeout,
                                         creation_task_exception_pb_bytes);
}

void CoreWorker::ForceExit(const rpc::WorkerExitType exit_type,
                           const std::string &detail) {
  RAY_LOG(DEBUG) << "ForceExit called: exit_type=" << static_cast<int>(exit_type)
                 << ", detail=" << detail;

  ShutdownReason reason = ConvertExitTypeToShutdownReason(exit_type, true);
  shutdown_coordinator_->RequestShutdown(
      /*force_shutdown=*/true, reason, detail, std::chrono::milliseconds{0}, nullptr);

  RAY_LOG(DEBUG) << "ForceExit: shutdown request completed";
}

const WorkerID &CoreWorker::GetWorkerID() const { return worker_context_->GetWorkerID(); }

void CoreWorker::SetCurrentTaskId(const TaskID &task_id,
                                  uint64_t attempt_number,
                                  const std::string &task_name) {
  worker_context_->SetCurrentTaskId(task_id, attempt_number);
  {
    absl::MutexLock lock(&mutex_);
    main_thread_task_id_ = task_id;
    main_thread_task_name_ = task_name;
  }
}

void CoreWorker::RegisterToGcs(int64_t worker_launch_time_ms,
                               int64_t worker_launched_time_ms) {
  absl::flat_hash_map<std::string, std::string> worker_info;
  const auto &worker_id = GetWorkerID();
  worker_info.emplace("node_ip_address", options_.node_ip_address);
  worker_info.emplace("plasma_store_socket", options_.store_socket);
  worker_info.emplace("raylet_socket", options_.raylet_socket);

  if (options_.worker_type == WorkerType::DRIVER) {
    auto start_time = clock_.NowUnixMillis();
    worker_info.emplace("driver_id", worker_id.Binary());
    worker_info.emplace("start_time", absl::StrCat(start_time));
    if (!options_.driver_name.empty()) {
      worker_info.emplace("name", options_.driver_name);
    }
  }

  auto worker_data = std::make_shared<rpc::WorkerTableData>();
  worker_data->mutable_worker_address()->set_node_id(rpc_address_.node_id());
  worker_data->mutable_worker_address()->set_ip_address(rpc_address_.ip_address());
  worker_data->mutable_worker_address()->set_port(rpc_address_.port());
  worker_data->mutable_worker_address()->set_worker_id(worker_id.Binary());

  worker_data->set_worker_type(options_.worker_type);
  worker_data->mutable_worker_info()->insert(std::make_move_iterator(worker_info.begin()),
                                             std::make_move_iterator(worker_info.end()));

  worker_data->set_is_alive(true);
  worker_data->set_pid(pid_);
  worker_data->set_start_time_ms(clock_.NowUnixMillis());
  worker_data->set_worker_launch_time_ms(worker_launch_time_ms);
  worker_data->set_worker_launched_time_ms(worker_launched_time_ms);

  gcs_client_->Workers().AsyncAdd(worker_data, nullptr);

  if (options_.worker_type == WorkerType::WORKER || recovery_succession_enabled_) {
    gcs_client_->Workers().AsyncSubscribeToWorkerFailures(
        [this](const rpc::WorkerDeltaData &worker_failure_data) {
          const WorkerID dead_worker =
              WorkerID::FromBinary(worker_failure_data.worker_id());

          if (options_.worker_type == WorkerType::WORKER) {
            HandleOwnerDied(dead_worker);
          }

          if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {
            recovery_succession_manager_->HandleWorkerFailure(dead_worker);
          }
        },
        nullptr);
  }
}

void CoreWorker::HandleOwnerDied(const WorkerID &dead_owner) {
  // Snapshot affected entries under the lock; act on them after releasing.
  struct DeadOwnerEntry {
    std::shared_ptr<TaskGeneratorBackpressureWaiter> waiter;
    std::shared_ptr<ActorTaskBackpressureMetadata> actor_metadata;
  };
  std::vector<DeadOwnerEntry> dead_entries;
  {
    absl::MutexLock lock(&mutex_);
    std::vector<ObjectID> to_erase;
    for (auto &[generator_id, state] : generator_backpressure_states_) {
      if (state.owner_worker_id == dead_owner) {
        dead_entries.push_back({state.waiter, state.actor_metadata});
        to_erase.push_back(generator_id);
        // Mark the gen task canceled so the executor loop bails before the
        // next gen.send instead of running another iteration of user code.
        canceled_tasks_.insert(generator_id.TaskId());
      }
    }
    for (const auto &generator_id : to_erase) {
      generator_backpressure_states_.erase(generator_id);
    }
  }
  for (auto &entry : dead_entries) {
    // Permanently disable per-task backpressure so the task can drain to its
    // natural exit instead of parking forever in WaitUntilObjectConsumed (the
    // dead owner will never send a consumption update) or WaitAllObjectsReported
    // (the in-flight report RPC may keep retrying until the client pool gives up).
    if (entry.waiter) {
      entry.waiter->DisableBackpressure();
    }
    if (entry.actor_metadata) {
      entry.actor_metadata->Teardown();
    }
  }
  // Wake every parked async streaming generator: the dead owner's tasks so they
  // exit their now-disabled per-task waits, and any actor-wide reserver since
  // the teardown above freed shared budget.
  if (!dead_entries.empty()) {
    NotifyAsyncGeneratorBackpressureUnblock(ObjectID::Nil(), /*notify_all=*/true);
  }
}

void CoreWorker::SubscribeToNodeChanges() {
  std::call_once(subscribe_to_node_changes_flag_, [this]() {
    // Register a callback to monitor add/removed nodes.
    // Note we capture a shared ownership of reference_counter, rate_limiter,
    // raylet_client_pool, and core_worker_client_pool here to avoid destruction order
    // fiasco between gcs_client, reference_counter_, raylet_client_pool_, and
    // core_worker_client_pool_.

    const std::weak_ptr<RecoverySuccessionManager> weak_recovery_manager =
        recovery_succession_manager_;


    const auto recovery_witness_node_cache =
        recovery_witness_node_cache_;

    const NodeID recovery_owner_node_id =
        GetCurrentNodeId();

    auto on_node_change =
    [reference_counter = reference_counter_,
     rate_limiter = lease_request_rate_limiter_,
     raylet_client_pool = raylet_client_pool_,
     core_worker_client_pool = core_worker_client_pool_,
     weak_recovery_manager,
     recovery_witness_node_cache,
     recovery_owner_node_id](
        const NodeID &node_id,
        const rpc::GcsNodeAddressAndLiveness &data) {



      {
        std::scoped_lock<std::mutex> lock(
            recovery_witness_node_cache->mutex);

        const bool valid_alive_witness =
            data.state() == rpc::GcsNodeInfo::ALIVE &&
            node_id != recovery_owner_node_id &&
            !data.node_manager_address().empty() &&
            data.node_manager_port() > 0;

        if (valid_alive_witness) {
          rpc::Address address;

          address.set_node_id(node_id.Binary());
          address.set_ip_address(
              data.node_manager_address());
          address.set_port(
              data.node_manager_port());

          recovery_witness_node_cache
              ->alive_nodes[node_id] =
              std::move(address);
        } else {
          recovery_witness_node_cache
              ->alive_nodes.erase(node_id);
        }
      }




      if (data.state() == rpc::GcsNodeInfo::DEAD) {
        RAY_LOG(INFO).WithField(node_id)
            << "Node failure. All objects pinned on that node will be lost if object "
               "reconstruction is not enabled.";
        reference_counter->ResetObjectsOnRemovedNode(node_id);
        raylet_client_pool->Disconnect(node_id);
        core_worker_client_pool->Disconnect(node_id);

        if (const auto recovery_manager = weak_recovery_manager.lock()) {
          recovery_manager->HandleNodeFailure(node_id);
        }
      }

      auto cluster_size_based_rate_limiter =
          dynamic_cast<ClusterSizeBasedLeaseRequestRateLimiter *>(rate_limiter.get());
      if (cluster_size_based_rate_limiter != nullptr) {
        cluster_size_based_rate_limiter->OnNodeChanges(data);
      }
    };

  gcs_client_->Nodes().AsyncSubscribeToNodeAddressAndLivenessChange(
      std::move(on_node_change),
      [this,
      recovery_witness_node_cache](
          const Status &status) {

        {
          std::scoped_lock<std::mutex> lock(
              gcs_client_node_cache_populated_mutex_);

          gcs_client_node_cache_populated_ = true;
        }

        gcs_client_node_cache_populated_cv_
            .notify_all();

        {
          std::scoped_lock<std::mutex> lock(
              recovery_witness_node_cache->mutex);

          recovery_witness_node_cache
              ->subscription_ok =
              status.ok();

          recovery_witness_node_cache
              ->initialized = true;
        }

        recovery_witness_node_cache
            ->cv.notify_all();

        if (!status.ok()) {
          RAY_LOG(WARNING)
              << "Failed to initialize recovery witness "
                "node cache: "
              << status;
        }
      });


  });
}

void CoreWorker::ExitIfParentRayletDies() {
  RAY_CHECK(!RayConfig::instance().RAYLET_PID().empty());
  static auto raylet_pid =
      static_cast<pid_t>(std::stoi(RayConfig::instance().RAYLET_PID()));
  bool should_shutdown = !IsProcessAlive(raylet_pid);
  if (should_shutdown) {
    RAY_LOG(WARNING) << "Shutting down the core worker because the local raylet failed. "
                     << "Check out the raylet.out log file. Raylet pid: " << raylet_pid;

    // Kill child procs so that child processes of the workers do not outlive the workers.
    KillChildProcs();

    QuickExit();
  }
}

void CoreWorker::InternalHeartbeat() {
  // Retry tasks.
  std::vector<TaskToRetry> tasks_to_resubmit;
  {
    absl::MutexLock lock(&mutex_);
    const auto current_time = clock_.SteadyNowMillis();
    while (!to_resubmit_.empty() && current_time > to_resubmit_.top().execution_time_ms) {
      tasks_to_resubmit.emplace_back(to_resubmit_.top());
      to_resubmit_.pop();
    }
  }

  for (auto &task_to_retry : tasks_to_resubmit) {
    auto &spec = task_to_retry.task_spec;
    if (spec.IsActorTask()) {
      auto actor_handle = actor_manager_->GetActorHandle(spec.ActorId());
      actor_handle->SetResubmittedActorTaskSpec(spec);
      actor_task_submitter_->SubmitTask(spec);
    } else if (spec.IsActorCreationTask()) {
      actor_task_submitter_->SubmitActorCreationTask(spec);
    } else {
      normal_task_submitter_->SubmitTask(spec);
    }
  }

  // Check timeout tasks that are waiting for death info.
  actor_task_submitter_->CheckTimeoutTasks();

  // Check for unhandled exceptions to raise after a timeout on the driver.
  // Only do this for TTY, since shells like IPython sometimes save references
  // to the result and prevent normal result deletion from handling.
  // See also: https://github.com/ray-project/ray/issues/14485
  if (options_.worker_type == WorkerType::DRIVER && options_.interactive) {
    memory_store_->NotifyUnhandledErrors();
  }
}

void CoreWorker::RecordMetrics() {
  // Record metrics for owned tasks.
  task_manager_->RecordMetrics();
  // Record metrics for executed tasks.
  task_counter_.RecordMetrics();
  // Record worker heap memory metrics.
  memory_store_->RecordMetrics();
  reference_counter_->RecordOwnerMetrics();
  // Flush percentile metrics: swap histogram buffers and update exported gauges.
  normal_task_submitter_->FlushMetrics();
}

std::unordered_map<ObjectID, std::pair<size_t, size_t>>
CoreWorker::GetAllReferenceCounts() const {
  auto counts = reference_counter_->GetAllReferenceCounts();
  std::vector<ObjectID> actor_handle_ids = actor_manager_->GetActorHandleIDsFromHandles();
  // Strip actor IDs from the ref counts since there is no associated ObjectID
  // in the language frontend.
  for (const auto &actor_handle_id : actor_handle_ids) {
    counts.erase(actor_handle_id);
  }
  return counts;
}



std::string
CoreWorker::GetRecoverySuccessionProfileJson() const {
  json result;

  result["profiling_enabled"] =
      recovery_succession_profiling_enabled_;

  result["witness_batch_ack_enabled"] = recovery_witness_ack_batch_handler_ != nullptr;
  const auto ack_stats = recovery_witness_ack_batch_handler_ != nullptr
      ? recovery_witness_ack_batch_handler_->GetStats() : RecoveryWitnessAckBatchStats{};
  result["witness_ack_batches_processed"] = ack_stats.batches;
  result["witness_ack_batch_items_processed"] = ack_stats.items;
  result["witness_ack_batch_bookkeeping_time_ns"] = ack_stats.bookkeeping_time_ns;
  result["witness_ack_batch_lock_wait_time_ns"] = ack_stats.lock_wait_time_ns;

  result["normal_submit_profile_calls"] =
      normal_submit_profile_calls_.load(std::memory_order_relaxed);
  result["normal_submit_prebuild_time_ns"] =
      normal_submit_prebuild_time_ns_.load(std::memory_order_relaxed);
  result["normal_submit_build_common_time_ns"] =
      normal_submit_build_common_time_ns_.load(std::memory_order_relaxed);
  result["normal_submit_finalize_spec_time_ns"] =
      normal_submit_finalize_spec_time_ns_.load(std::memory_order_relaxed);
  result["normal_submit_add_pending_time_ns"] =
      normal_submit_add_pending_time_ns_.load(std::memory_order_relaxed);
  result["normal_submit_owner_setup_time_ns"] =
      normal_submit_owner_setup_time_ns_.load(std::memory_order_relaxed);
  result["normal_submit_dispatch_setup_time_ns"] =
      normal_submit_dispatch_setup_time_ns_.load(std::memory_order_relaxed);
  result["normal_submit_total_time_ns"] =
      normal_submit_total_time_ns_.load(std::memory_order_relaxed);

  if (!recovery_succession_profiling_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    return result.dump();
  }

  const auto profile =
      recovery_succession_manager_
          ->GetProfileSnapshot();

  result["candidate_reports_received"] =
      profile.candidate_reports_received;
  result["candidate_reports_accepted"] =
      profile.candidate_reports_accepted;
      
  result["holder_install_rpcs_sent"] =
      profile.holder_install_rpcs_sent;
  result["holder_install_rpcs_completed"] =
      profile.holder_install_rpcs_completed;

  result["holder_commit_rpcs_sent"] =
      profile.holder_commit_rpcs_sent;
  result["holder_commit_rpcs_completed"] =
      profile.holder_commit_rpcs_completed;

  result["witness_update_rpcs_sent"] =
      profile.witness_update_rpcs_sent;
  result["witness_update_rpcs_completed"] =
      profile.witness_update_rpcs_completed;
  result["witness_update_client_queue_time_ns"] =
      profile.witness_update_client_queue_time_ns;
  result["witness_update_client_submit_to_cq_time_ns"] =
      profile.witness_update_client_submit_to_cq_time_ns;
  result["witness_update_client_cq_to_main_loop_time_ns"] =
      profile.witness_update_client_cq_to_main_loop_time_ns;
  result["witness_update_client_main_loop_to_batch_callback_time_ns"] =
      profile.witness_update_client_main_loop_to_batch_callback_time_ns;
  result["witness_update_client_phase_samples"] =
      profile.witness_update_client_phase_samples;
  result["witness_update_server_batch_queue_time_ns"] =
      profile.witness_update_server_batch_queue_time_ns;
  result["witness_update_handler_time_ns"] =
      profile.witness_update_handler_time_ns;
  result["witness_update_handler_samples"] =
      profile.witness_update_handler_samples;
  result["witness_update_mutex_wait_time_ns"] =
      profile.witness_update_mutex_wait_time_ns;
  result["witness_update_mutex_hold_time_ns"] =
      profile.witness_update_mutex_hold_time_ns;
  result["witness_update_physical_batches_completed"] =
      profile.witness_update_physical_batches_completed;
  result["witness_update_physical_batch_items"] =
      profile.witness_update_physical_batch_items;
  result["witness_update_client_enqueue_cpu_time_ns"] =
      profile.witness_update_client_enqueue_cpu_time_ns;
  result["witness_update_client_batch_build_cpu_time_ns"] =
      profile.witness_update_client_batch_build_cpu_time_ns;
  result["witness_update_client_batch_demux_cpu_time_ns"] =
      profile.witness_update_client_batch_demux_cpu_time_ns;
  result["holder_admission_prepare_cpu_calls"] =
      profile.holder_admission_prepare_cpu_calls;
  result["holder_admission_prepare_cpu_time_ns"] =
      profile.holder_admission_prepare_cpu_time_ns;
  result["witness_request_build_cpu_calls"] =
      profile.witness_request_build_cpu_calls;
  result["witness_request_build_cpu_time_ns"] =
      profile.witness_request_build_cpu_time_ns;
  result["witness_logical_callback_cpu_calls"] =
      profile.witness_logical_callback_cpu_calls;
  result["witness_logical_callback_cpu_time_ns"] =
      profile.witness_logical_callback_cpu_time_ns;
  result["witness_winner_callback_cpu_calls"] =
      profile.witness_winner_callback_cpu_calls;
  result["witness_winner_callback_cpu_time_ns"] =
      profile.witness_winner_callback_cpu_time_ns;
  result["witness_redundant_callback_cpu_calls"] =
      profile.witness_redundant_callback_cpu_calls;
  result["witness_redundant_callback_cpu_time_ns"] =
      profile.witness_redundant_callback_cpu_time_ns;
  result["holder_commit_cpu_calls"] =
      profile.holder_commit_cpu_calls;
  result["holder_commit_cpu_time_ns"] =
      profile.holder_commit_cpu_time_ns;
  result["h1_publish_readiness_samples"] =
      profile.h1_publish_readiness_samples;
  result["h2_reserved_at_h1_publish"] =
      profile.h2_reserved_at_h1_publish;
  result["h2_installed_at_h1_publish"] =
      profile.h2_installed_at_h1_publish;
  result["h1_ack_readiness_samples"] =
      profile.h1_ack_readiness_samples;
  result["h2_reserved_at_h1_ack"] =
      profile.h2_reserved_at_h1_ack;
  result["h2_installed_at_h1_ack"] =
      profile.h2_installed_at_h1_ack;

  result["task_spec_bytes_sent"] =
      profile.task_spec_bytes_sent;
  result["manifest_bytes_sent"] =
      profile.manifest_bytes_sent;

  result["owner_task_spec_copy_count"] =
      profile.owner_task_spec_copy_count;
  result["owner_task_spec_copy_time_ns"] =
      profile.owner_task_spec_copy_time_ns;
  result["owner_lazy_task_spec_copies_avoided"] =
      profile.owner_lazy_task_spec_copies_avoided;

  result["owner_retained_task_specs_current"] =
      profile.owner_retained_task_specs_current;
  result["owner_retained_task_specs_peak"] =
      profile.owner_retained_task_specs_peak;
  result["owner_retained_task_spec_bytes_current"] =
      profile.owner_retained_task_spec_bytes_current;
  result["owner_retained_task_spec_bytes_peak"] =
      profile.owner_retained_task_spec_bytes_peak;
  result["owner_retained_task_specs_created"] =
      profile.owner_retained_task_specs_created;
  result["owner_retained_task_specs_released"] =
      profile.owner_retained_task_specs_released;
  result["owner_retained_task_spec_copy_time_ns"] =
      profile.owner_retained_task_spec_copy_time_ns;

  result["task_centric_metadata_builds"] =
      profile.task_centric_metadata_builds;
  // Patch 4F TaskSpecs transported through normal downstream PushTask.
  result["first_holder_piggyback_copies_sent"] =
      profile.first_holder_piggyback_copies_sent;
  result["first_holder_piggyback_bytes_sent"] =
      profile.first_holder_piggyback_bytes_sent;
  result["first_holder_piggyback_serialize_time_ns"] =
      profile.first_holder_piggyback_serialize_time_ns;

  result["initial_install_profile_version"] = 3;
  result["frontier_recipe_piggybacks_sent"] =
      profile.frontier_recipe_piggybacks_sent;
  result["frontier_recipe_piggyback_bytes_sent"] =
      profile.frontier_recipe_piggyback_bytes_sent;
  result["frontier_recipe_piggybacks_stored"] =
      profile.frontier_recipe_piggybacks_stored;
  result["frontier_recipe_piggyback_store_time_ns"] =
      profile.frontier_recipe_piggyback_store_time_ns;
  result["frontier_recipe_piggyback_admissions"] =
      profile.frontier_recipe_piggyback_admissions;
  result["frontier_recipe_encode_calls"] =
      profile.frontier_recipe_encode_calls;
  result["frontier_recipe_encode_time_ns"] =
      profile.frontier_recipe_encode_time_ns;
  result["frontier_recipe_encode_members"] =
      profile.frontier_recipe_encode_members;
  result["frontier_recipe_encode_bytes"] =
      profile.frontier_recipe_encode_bytes;
  result["holder_install_handler_calls"] =
      profile.holder_install_handler_calls;
  result["holder_install_handler_time_ns"] =
      profile.holder_install_handler_time_ns;
  result["frontier_holder_materialize_calls"] =
      profile.frontier_holder_materialize_calls;
  result["frontier_holder_materialize_time_ns"] =
      profile.frontier_holder_materialize_time_ns;
  result["frontier_holder_materialize_members"] =
      profile.frontier_holder_materialize_members;
  result["holder_install_callback_calls"] =
      profile.holder_install_callback_calls;
  result["holder_install_callback_time_ns"] =
      profile.holder_install_callback_time_ns;

  result["holder_install_rpc_time_ns"] =
      profile.holder_install_rpc_time_ns;
  result["holder_commit_rpc_time_ns"] =
      profile.holder_commit_rpc_time_ns;
  result["witness_update_rpc_time_ns"] =
      profile.witness_update_rpc_time_ns;

  result["witness_publish_count"] =
    profile.witness_publish_count;
  result["witness_publish_time_ns"] =
      profile.witness_publish_time_ns;
  result["witness_publish_max_time_ns"] =
      profile.witness_publish_max_time_ns;

  result["holder_admissions_committed"] =
      profile.holder_admissions_committed;
  result["holder_admission_time_ns"] =
      profile.holder_admission_time_ns;
  result["holder_admission_max_time_ns"] =
      profile.holder_admission_max_time_ns;

  result["manifest_generations_committed"] =
      profile.manifest_generations_committed;
  result["max_generation"] =
      profile.max_generation;
  result["max_non_owner_holders"] =
      profile.max_non_owner_holders;
  result["frozen_commits"] =
      profile.frozen_commits;





  result["task_argument_metadata_calls"] =
      profile.task_argument_metadata_calls;
  result["task_argument_metadata_time_ns"] =
      profile.task_argument_metadata_time_ns;

  result["task_argument_metadata_refs_attached"] =
      profile.task_argument_metadata_refs_attached;
  result["task_argument_metadata_compact_refs"] =
      profile.task_argument_metadata_compact_refs;
  result["task_argument_metadata_compact_fallbacks"] =
      profile.task_argument_metadata_compact_fallbacks;
  result["task_argument_metadata_full_bytes_equivalent"] =
      profile.task_argument_metadata_full_bytes_equivalent;
  result["task_argument_metadata_transport_bytes"] =
      profile.task_argument_metadata_transport_bytes;

  result["initial_manifest_build_count"] =
      profile.initial_manifest_build_count;
  result["initial_manifest_build_time_ns"] =
      profile.initial_manifest_build_time_ns;
  result["initial_manifest_bytes"] =
      profile.initial_manifest_bytes;

  result["witness_selection_count"] =
      profile.witness_selection_count;
  result["witness_selection_time_ns"] =
      profile.witness_selection_time_ns;

  result["witness_gcs_query_count"] =
      profile.witness_gcs_query_count;
  result["witness_gcs_query_time_ns"] =
      profile.witness_gcs_query_time_ns;

  result["task_spec_manifest_attach_count"] =
      profile.task_spec_manifest_attach_count;
  result["task_spec_manifest_attach_time_ns"] =
      profile.task_spec_manifest_attach_time_ns;

  result["register_owned_task_count"] =
      profile.register_owned_task_count;
  result["register_owned_task_time_ns"] =
      profile.register_owned_task_time_ns;


  result["recovery_metadata_lookup_calls"] = profile.recovery_metadata_lookup_calls;
  result["recovery_metadata_lookup_hits"] = profile.recovery_metadata_lookup_hits;
  result["recovery_metadata_lookup_time_ns"] = profile.recovery_metadata_lookup_time_ns;
  result["ensure_task_arguments_calls"] = profile.ensure_task_arguments_calls;
  result["ensure_task_arguments_time_ns"] = profile.ensure_task_arguments_time_ns;
  result["register_executor_task_calls"] = profile.register_executor_task_calls;
  result["register_executor_task_time_ns"] = profile.register_executor_task_time_ns;
  result["register_executor_metadata_refs_seen"] =
      profile.register_executor_metadata_refs_seen;
  result["register_executor_candidate_reports_built"] =
      profile.register_executor_candidate_reports_built;
  result["candidate_report_build_calls"] = profile.candidate_report_build_calls;
  result["candidate_reports_built"] = profile.candidate_reports_built;
  result["candidate_report_build_time_ns"] = profile.candidate_report_build_time_ns;
  result["candidate_queue_calls"] = profile.candidate_queue_calls;
  result["candidate_queue_time_ns"] = profile.candidate_queue_time_ns;
  result["candidate_rpc_logical_reports_sent"] =
      profile.candidate_rpc_logical_reports_sent;
  result["candidate_rpc_logical_reports_completed"] =
      profile.candidate_rpc_logical_reports_completed;
  result["candidate_rpc_physical_rpcs_sent"] =
      profile.candidate_rpc_physical_rpcs_sent;
  result["candidate_rpc_physical_rpcs_completed"] =
      profile.candidate_rpc_physical_rpcs_completed;
  result["candidate_rpc_request_bytes_sent"] =
      profile.candidate_rpc_request_bytes_sent;
  result["candidate_rpc_time_ns"] = profile.candidate_rpc_time_ns;




  return result.dump();
}

void CoreWorker::ResetRecoverySuccessionProfile() {
  normal_submit_profile_calls_.store(0, std::memory_order_relaxed);
  normal_submit_prebuild_time_ns_.store(0, std::memory_order_relaxed);
  normal_submit_build_common_time_ns_.store(0, std::memory_order_relaxed);
  normal_submit_finalize_spec_time_ns_.store(0, std::memory_order_relaxed);
  normal_submit_add_pending_time_ns_.store(0, std::memory_order_relaxed);
  normal_submit_owner_setup_time_ns_.store(0, std::memory_order_relaxed);
  normal_submit_dispatch_setup_time_ns_.store(0, std::memory_order_relaxed);
  normal_submit_total_time_ns_.store(0, std::memory_order_relaxed);

  if (recovery_succession_manager_ != nullptr) {
    recovery_succession_manager_->ResetProfile();
  }
  if (recovery_witness_ack_batch_handler_ != nullptr) {
    recovery_witness_ack_batch_handler_->ResetStats();
  }
}

std::vector<TaskID> CoreWorker::GetPendingChildrenTasks(const TaskID &task_id) const {
  return task_manager_->GetPendingChildrenTasks(task_id);
}

const rpc::Address &CoreWorker::GetRpcAddress() const { return rpc_address_; }

bool CoreWorker::HasOwner(const ObjectID &object_id) const {
  return reference_counter_->HasOwner(object_id);
}

rpc::Address CoreWorker::GetOwnerAddressOrDie(const ObjectID &object_id) const {
  rpc::Address owner_address;
  auto status = GetOwnerAddress(object_id, &owner_address);
  RAY_CHECK_OK(status);
  return owner_address;
}

Status CoreWorker::GetOwnerAddress(const ObjectID &object_id,
                                   rpc::Address *owner_address) const {
  auto has_owner = reference_counter_->GetOwner(object_id, owner_address);
  if (!has_owner) {
    std::ostringstream stream;
    stream << "An application is trying to access a Ray object whose owner is unknown"
           << "(" << object_id
           << "). "
              "Please make sure that all Ray objects you are trying to access are part"
              " of the current Ray session. Note that "
              "object IDs generated randomly (ObjectID.from_random()) or out-of-band "
              "(ObjectID.from_binary(...)) cannot be passed as a task argument because"
              " Ray does not know which task created them. "
              "If this was not how your object ID was generated, please file an issue "
              "at https://github.com/ray-project/ray/issues/";
    return Status::ObjectUnknownOwner(stream.str());
  }
  return Status::OK();
}


bool CoreWorker::TryPopulateRecoveryMetadataForObject(
    const ObjectID &object_id,
    rpc::RecoveryObjectMetadata *metadata,
    std::vector<DeferredRecoveryFrontierGroup> *deferred_groups) const {
  if (!recovery_succession_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    return false;
  }

  // Patch 4H: callers that only need activation may pass nullptr. Avoid a
  // complete RecoveryObjectMetadata/RecoveryManifest CopyFrom on that path.
  if (metadata == nullptr) {
    if (recovery_succession_manager_->HasRecoveryMetadata(object_id)) {
      return true;
    }
  } else if (recovery_succession_manager_->PopulateRecoveryMetadata(
                 object_id, metadata)) {
    return true;
  }

  // Both Succession and the fixed witness-holder baseline are activated
  // lazily on the first real export/borrow of an eligible task return.

  const TaskID task_id = object_id.TaskId();

  // Preserve transitive replay correctness for immediate task chains.
  WaitForDeferredRecoveryTaskDependencies(task_id);

  const bool recovery_frontier_enabled =
      recovery_witness_holder_baseline_enabled_ &&
      recovery_succession_manager_->RecoveryFrontierEnabled();

  // PERF-ONLY frontier-density selector.
  //
  // For K>1, only approximately 1/K baseline tasks pay the protection cost.
  // Selection is deterministic by TaskID so repeated exports of the same
  // object make the same decision without shared counters or synchronization.
  //
  // Returning false here exports the ordinary ObjectRef without recovery
  // metadata. This is intentionally NOT recovery-correct for K>1.
  if (recovery_witness_holder_baseline_enabled_ &&
      !recovery_frontier_enabled) {
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
        return false;
      }
    }
  }

  auto task_spec_opt = task_manager_->GetTaskSpec(task_id);

  if (!task_spec_opt.has_value()) {
    // Patch 4L: producer completion may have removed TaskManager's ordinary
    // lineage even though the returned ObjectRef is still strongly live.
    rpc::TaskSpec retained_task_spec;
    if (!recovery_succession_manager_->GetRetainedOwnerTaskSpec(
            task_id, &retained_task_spec)) {
      return false;
    }

    task_spec_opt.emplace(std::move(retained_task_spec));
  }

  const TaskSpecification &task_spec = task_spec_opt.value();
  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  if (!RecoverySuccessionManager::IsEligibleTask(task_proto) ||
      task_proto.task_id().empty()) {
    return false;
  }

  // Protect only static task returns, never ray.put() objects or actor handles.
  bool is_static_return = false;
  for (size_t return_index = 0; return_index < task_spec.NumReturns();
       ++return_index) {
    if (task_spec.ReturnId(return_index) == object_id) {
      is_static_return = true;
      break;
    }
  }

  if (!is_static_return) {
    return false;
  }

  const bool recovery_frontier_grouping_enabled =
      recovery_frontier_enabled &&
      RayConfig::instance().recovery_frontier_group_size() > 1;

  std::optional<RecoveryFrontierMembership> frontier_membership;
  rpc::RecoveryManifest frontier_protection_manifest;
  rpc::RecoveryManifest manifest;

  if (recovery_frontier_grouping_enabled) {
    frontier_membership =
        recovery_succession_manager_->GetRecoveryFrontierMembership(task_id);
    if (!frontier_membership.has_value()) {
      frontier_membership =
          recovery_succession_manager_->RegisterOwnerTaskWithRecoveryFrontier(
              task_spec);
    }
    if (!frontier_membership.has_value()) {
      return false;
    }

    if (!recovery_succession_manager_->GetRecoveryFrontierProtectionManifest(
            frontier_membership->group_id, &frontier_protection_manifest)) {
      const uint64_t manifest_start_ns =
          recovery_succession_profiling_enabled_
              ? RecoveryProfileNowNs()
              : 0;

      rpc::RecoveryManifest candidate =
          recovery_succession_manager_->BuildInitialManifest(
              frontier_membership->group_id,
              task_spec.JobId(),
              task_proto.max_retries());

      if (manifest_start_ns != 0) {
        recovery_succession_manager_->RecordInitialManifestBuild(
            RecoveryProfileNowNs() - manifest_start_ns,
            static_cast<uint64_t>(candidate.ByteSizeLong()));
      }

      const uint64_t witness_start_ns =
          recovery_succession_profiling_enabled_
              ? RecoveryProfileNowNs()
              : 0;
      PopulateRecoveryWitnesses(&candidate);
      if (witness_start_ns != 0) {
        recovery_succession_manager_->RecordWitnessSelectionLatency(
            RecoveryProfileNowNs() - witness_start_ns);
      }

      RAY_CHECK(
          recovery_succession_manager_->CacheRecoveryFrontierProtectionManifest(
              candidate, &frontier_protection_manifest))
          << "Failed to cache Recovery Frontier protection topology for group "
          << frontier_membership->group_id;
    }

    // Manager/object metadata remains task-centric so borrowers continue to
    // request the original deterministic TaskID. Only holder storage/publication
    // is grouped under the frontier leader.
    manifest.CopyFrom(frontier_protection_manifest);
    manifest.set_task_id(task_id.Binary());
    manifest.set_job_id(task_spec.JobId().Binary());
    manifest.set_max_recovery_attempts(task_proto.max_retries());
  } else {
    uint64_t manifest_start_ns = 0;
    if (recovery_succession_profiling_enabled_) {
      manifest_start_ns = RecoveryProfileNowNs();
    }

    manifest = recovery_succession_manager_->BuildInitialManifest(
        task_id, task_spec.JobId(), task_proto.max_retries());

    if (manifest_start_ns != 0) {
      recovery_succession_manager_->RecordInitialManifestBuild(
          RecoveryProfileNowNs() - manifest_start_ns,
          static_cast<uint64_t>(manifest.ByteSizeLong()));
    }

    uint64_t witness_start_ns = 0;
    if (recovery_succession_profiling_enabled_) {
      witness_start_ns = RecoveryProfileNowNs();
    }

    PopulateRecoveryWitnesses(&manifest);

    if (witness_start_ns != 0) {
      recovery_succession_manager_->RecordWitnessSelectionLatency(
          RecoveryProfileNowNs() - witness_start_ns);
    }
  }

  uint64_t register_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    register_start_ns = RecoveryProfileNowNs();
  }

  const bool initialized_now =
      recovery_succession_manager_->RegisterOwnedTaskLazy(task_spec, manifest);

  if (register_start_ns != 0 && initialized_now) {
    recovery_succession_manager_->RecordRegisterOwnedTaskLatency(
        RecoveryProfileNowNs() - register_start_ns);
  }

  if (recovery_witness_holder_baseline_enabled_) {
    if (recovery_frontier_grouping_enabled) {
      RAY_CHECK(frontier_membership.has_value());

      // The initial metadata fast-path above already established that this
      // export still needs activation. PublishRecoveryFrontierGroup() is the
      // acknowledged-prefix visibility barrier: it returns only after this
      // group's currently registered members are durable on every fixed-R
      // holder. Re-checking HasRecoveryMetadata() around that barrier adds
      // manager lock/hash lookups without adding a correctness check.
      // Racing exporters safely join the same barrier.
      if (deferred_groups != nullptr) {
        RAY_CHECK(metadata == nullptr)
            << "Deferred Recovery Frontier preparation must remain owner-local";

        const bool already_pending = std::any_of(
            deferred_groups->begin(),
            deferred_groups->end(),
            [&frontier_membership](const DeferredRecoveryFrontierGroup &group) {
              return group.group_id == frontier_membership->group_id;
            });
        if (!already_pending) {
          DeferredRecoveryFrontierGroup group;
          group.group_id = frontier_membership->group_id;
          group.protection_manifest.CopyFrom(frontier_protection_manifest);
          deferred_groups->push_back(std::move(group));
        }
        return true;
      }

      PublishRecoveryFrontierGroup(
          frontier_membership->group_id, frontier_protection_manifest);

      if (metadata == nullptr) {
        return true;
      }
      return recovery_succession_manager_->PopulateRecoveryMetadata(
          object_id, metadata);
    } else if (initialized_now) {
    const uint32_t target_holder_count =
        RayConfig::instance().recovery_succession_target_holder_count();

    RAY_CHECK_EQ(
        static_cast<uint32_t>(manifest.witness_raylets_size()),
        target_holder_count)
        << "Lazy witness-holder baseline requires exactly "
        << target_holder_count
        << " independent full-lineage witnesses, but only "
        << manifest.witness_raylets_size()
        << " were selected.";

    RAY_CHECK_EQ(manifest.witness_count(), target_holder_count);

    const bool serialize_task_spec_once =
        RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once();

    rpc::TaskSpec serialized_task_spec_proto;
    std::string serialized_baseline_task_spec;
    const rpc::TaskSpec *publish_task_spec = nullptr;
    const std::string *publish_serialized_task_spec = nullptr;

    if (serialize_task_spec_once) {
      // Experimental crossover path. The wire contract remains a complete
      // replayable TaskSpec with the authoritative RecoveryManifest embedded.
      serialized_task_spec_proto.CopyFrom(task_proto);
      serialized_task_spec_proto.mutable_recovery_manifest()->CopyFrom(manifest);
      serialized_baseline_task_spec = serialized_task_spec_proto.SerializeAsString();
      publish_serialized_task_spec = &serialized_baseline_task_spec;
    } else {
      // Frozen baseline: publication copies directly into each outgoing request.
      publish_task_spec = &task_proto;
    }

    const uint64_t publish_start_ns =
        recovery_succession_profiling_enabled_
            ? RecoveryProfileNowNs()
            : 0;

    PublishRecoveryManifestToWitnesses(
        manifest,
        [manager = recovery_succession_manager_,
         task_id,
         publish_start_ns](
            bool stored,
            std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
          if (publish_start_ns != 0) {
            manager->RecordWitnessPublishLatency(
                RecoveryProfileNowNs() - publish_start_ns);
          }

          if (!stored) {
            // A completed task may legitimately leave application scope while
            // this baseline's R full-TaskSpec witness-holder writes are still
            // in flight. Patch 4L cleanup then publishes a newer tombstone.
            // Any witness that sees that tombstone first must reject this older
            // install. That is cancellation/supersession, not a durability
            // failure, because the object no longer needs protection.
            //
            // Keep every other failure fatal so this does NOT weaken the
            // live-reference requirement that all R baseline holders store the
            // full TaskSpec while the producer ObjectRef remains live.
            if (newer_manifest.has_value() &&
                newer_manifest->tombstoned()) {
              manager->ApplyRecoveryTombstone(
                  newer_manifest.value());

              RAY_LOG(DEBUG)
                  .WithField(task_id)
                  << "Lazy witness-holder baseline install was superseded by "
                     "a newer tombstone at generation "
                  << newer_manifest->version().generation();
              return;
            }

            RAY_LOG(FATAL)
                .WithField(task_id)
                << "Lazy witness-holder baseline failed to install "
                << "the full TaskSpec on every configured holder."
                << (newer_manifest.has_value()
                        ? " A newer non-tombstone witness manifest was observed."
                        : "");
          }

          RAY_LOG(INFO)
              .WithField(task_id)
              << "Installed full TaskSpec on all "
                 "witness-holder baseline nodes";
        },
        publish_task_spec,
        publish_serialized_task_spec);
    }
  }

  // If another thread won the initialization race, its metadata is visible
  // here once RegisterOwnedTaskLazy returns.
  if (metadata == nullptr) {
    return recovery_succession_manager_->HasRecoveryMetadata(object_id);
  }
  return recovery_succession_manager_->PopulateRecoveryMetadata(
      object_id, metadata);
}

void CoreWorker::EnsureRecoverySuccessionForTaskArguments(
    rpc::TaskSpec *task_spec,
    std::vector<DeferredRecoveryFrontierGroup> *deferred_groups) const {
  if (task_spec == nullptr || !recovery_succession_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    return;
  }

  const uint64_t patch4g_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
  for (const rpc::TaskArg &arg : task_spec->args()) {
    if (arg.has_object_ref() && !arg.object_ref().object_id().empty()) {
      const ObjectID object_id =
          ObjectID::FromBinary(arg.object_ref().object_id());
      TryPopulateRecoveryMetadataForObject(
          object_id, nullptr, deferred_groups);
    }

    for (const rpc::ObjectReference &nested_ref :
         arg.nested_inlined_refs()) {
      if (nested_ref.object_id().empty()) {
        continue;
      }

      const ObjectID nested_id =
          ObjectID::FromBinary(nested_ref.object_id());
      TryPopulateRecoveryMetadataForObject(
          nested_id, nullptr, deferred_groups);
    }
  }

  if (patch4g_start_ns != 0) {
    recovery_succession_manager_->RecordEnsureTaskArgumentsLatency(
        RecoveryProfileNowNs() - patch4g_start_ns);
  }
}

void CoreWorker::WaitForDeferredRecoveryTaskDependencies(
    const TaskID &task_id) const {
  std::unique_lock<std::mutex> lock(recovery_frontier_deferred_task_mutex_);
  recovery_frontier_deferred_task_cv_.wait(lock, [this, &task_id] {
    return recovery_frontier_deferred_tasks_.find(task_id) ==
           recovery_frontier_deferred_tasks_.end();
  });
}

void CoreWorker::CompleteDeferredRecoveryFrontierGroup(
    const TaskID &group_id) const {
  // One allocation per completed group, rather than one readiness object,
  // atomic counter, and TaskSpecification shared_ptr per downstream task.
  auto ready_tasks = std::make_shared<std::vector<TaskSpecification>>();

  {
    std::lock_guard<std::mutex> lock(recovery_frontier_deferred_task_mutex_);
    const auto group_it =
        recovery_frontier_deferred_group_waiters_.find(group_id);
    if (group_it == recovery_frontier_deferred_group_waiters_.end()) {
      return;
    }

    ready_tasks->reserve(group_it->second.size());
    for (const TaskID &task_id : group_it->second) {
      auto task_it = recovery_frontier_deferred_tasks_.find(task_id);
      RAY_CHECK(task_it != recovery_frontier_deferred_tasks_.end())
          << "Missing deferred Recovery Frontier task state for " << task_id
          << " while completing group " << group_id;
      RAY_CHECK_GT(task_it->second.remaining_groups, 0u);

      --task_it->second.remaining_groups;
      if (task_it->second.remaining_groups != 0) {
        continue;
      }

      RAY_CHECK(task_it->second.task_to_dispatch.has_value());
      ready_tasks->push_back(
          std::move(*task_it->second.task_to_dispatch));
      recovery_frontier_deferred_tasks_.erase(task_it);
    }

    recovery_frontier_deferred_group_waiters_.erase(group_it);
  }

  // Erasing a task from the shared table is the readiness transition observed
  // by chained submissions. Wake them before posting the now-safe dispatches.
  recovery_frontier_deferred_task_cv_.notify_all();

  if (ready_tasks->empty()) {
    return;
  }

  io_service_.post(
      [this, ready_tasks]() mutable {
        for (TaskSpecification &task : *ready_tasks) {
          normal_task_submitter_->SubmitTask(std::move(task));
        }
      },
      "CoreWorker.SubmitTasksAfterRecoveryFrontierCommit");
}


std::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs(
    const std::vector<ObjectID> &object_ids,
    bool task_argument_serialization) const {
  std::vector<rpc::ObjectReference> refs;
  refs.reserve(object_ids.size());

  const bool defer_frontier_task_argument_activation =
      task_argument_serialization &&
      recovery_witness_holder_baseline_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      recovery_succession_manager_->RecoveryFrontierEnabled() &&
      RayConfig::instance().recovery_frontier_group_size() > 1;

  for (const auto &object_id : object_ids) {
    rpc::ObjectReference ref;
    ref.set_object_id(object_id.Binary());

    rpc::Address owner_address;
    if (reference_counter_->GetOwner(object_id, &owner_address)) {
      // NOTE(swang): Detached actors do not have an
      // owner address set.
      *ref.mutable_owner_address() = std::move(owner_address);
    }

    if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {
      rpc::RecoveryObjectMetadata metadata;

      if (defer_frontier_task_argument_activation) {
        // Nested ObjectRefs in a by-value task argument are seen again by
        // BuildCommonTaskSpec via nested_inlined_refs().  For K>1, preserve
        // already-committed metadata here without starting a synchronous
        // publication. BuildCommonTaskSpec will activate any uncommitted group
        // through its deferred-group path and gate remote dispatch on the same
        // all-R durability ACK.
        if (recovery_succession_manager_->PopulateRecoveryMetadata(object_id,
                                                                   &metadata)) {
          ref.mutable_recovery_metadata()->CopyFrom(metadata);
        }
      } else if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {
        ref.mutable_recovery_metadata()->CopyFrom(metadata);
      }
    }

    refs.emplace_back(std::move(ref));
  }

  return refs;
}

Status CoreWorker::GetOwnershipInfo(const ObjectID &object_id,
                                    rpc::Address *owner_address,
                                    std::string *serialized_object_status,
                                    bool task_argument_serialization) {
  auto has_owner = reference_counter_->GetOwner(object_id, owner_address);
  if (!has_owner) {
    std::ostringstream stream;
    stream << "An application is trying to access a Ray object whose owner is unknown"
           << "(" << object_id
           << "). "
              "Please make sure that all Ray objects you are trying to access are part"
              " of the current Ray session. Note that "
              "object IDs generated randomly (ObjectID.from_random()) or out-of-band "
              "(ObjectID.from_binary(...)) cannot be passed as a task argument because"
              " Ray does not know which task created them. "
              "If this was not how your object ID was generated, please file an issue "
              "at https://github.com/ray-project/ray/issues/";
    return Status::ObjectUnknownOwner(stream.str());
  }

  rpc::GetObjectStatusReply object_status;
  // Optimization: if the object exists, serialize and inline its status. This also
  // resolves some race conditions in resource release (#16025).
  auto existing_object = memory_store_->GetIfExists(object_id);

  if (existing_object != nullptr) {
    PopulateObjectStatus(object_id, existing_object, &object_status);
  }

  if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {
    rpc::RecoveryObjectMetadata metadata;

    // Nested ObjectRefs serialized as task arguments are seen again by
    // BuildCommonTaskSpec via nested_inlined_refs(). For Fixed-R Frontier K>1,
    // do not synchronously cross the all-R durability barrier here; the task
    // builder attaches the owner-local recovery sidecar and gates remote dispatch
    // on the same Frontier ACK. Explicit/out-of-band serialization, Fixed-R K1,
    // and Succession retain the original blocking visibility contract.
    const bool defer_frontier_task_argument_activation =
        task_argument_serialization &&
        recovery_witness_holder_baseline_enabled_ &&
        recovery_succession_manager_->RecoveryFrontierEnabled() &&
        RayConfig::instance().recovery_frontier_group_size() > 1;

    if (defer_frontier_task_argument_activation) {
      // Preserve already-committed metadata without activating new protection.
      // PopulateRecoveryMetadata exposes only committed recovery state.
      if (recovery_succession_manager_->PopulateRecoveryMetadata(object_id, &metadata)) {
        object_status.mutable_recovery_metadata()->CopyFrom(metadata);
      }
    } else if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {
      object_status.mutable_recovery_metadata()->CopyFrom(metadata);
    }
  }

  *serialized_object_status = object_status.SerializeAsString();

  return Status::OK();
}

void CoreWorker::RegisterOwnershipInfoAndResolveFuture(
    const ObjectID &object_id,
    const ObjectID &outer_object_id,
    const rpc::Address &owner_address,
    const std::string &serialized_object_status) {
  // Add the object's owner to the local metadata in case it gets serialized
  // again.
  reference_counter_->AddBorrowedObject(object_id, outer_object_id, owner_address);

  rpc::GetObjectStatusReply object_status;
  object_status.ParseFromString(serialized_object_status);

  if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr &&
      object_status.has_recovery_metadata()) {
    recovery_succession_manager_->RegisterBorrowedObject(
        object_id, object_status.recovery_metadata());
  }

  if (object_status.has_object() && !reference_counter_->OwnedByUs(object_id)) {
    // We already have the inlined object status, process it immediately.
    future_resolver_->ProcessResolvedObject(
        object_id, owner_address, Status::OK(), object_status);
  } else {
    // We will ask the owner about the object until the object is
    // created or we can no longer reach the owner.
    future_resolver_->ResolveFutureAsync(object_id, owner_address);
  }
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       ObjectID *object_id) {
  SubscribeToNodeChanges();
  *object_id = ObjectID::FromIndex(worker_context_->GetCurrentInternalTaskId(),
                                   worker_context_->GetNextPutIndex());
  reference_counter_->AddOwnedObject(*object_id,
                                     contained_object_ids,
                                     rpc_address_,
                                     CurrentCallSite(),
                                     object.GetSize(),
                                     LineageReconstructionEligibility::INELIGIBLE_PUT,
                                     /*add_local_ref=*/true,
                                     NodeID::FromBinary(rpc_address_.node_id()));
  auto status = Put(object, contained_object_ids, *object_id, /*pin_object=*/true);
  if (!status.ok()) {
    RemoveLocalReference(*object_id);
  }
  return status;
}

Status CoreWorker::PutInLocalPlasmaStore(const RayObject &object,
                                         const ObjectID &object_id,
                                         bool pin_object) {
  bool object_exists = false;
  RAY_RETURN_NOT_OK(plasma_store_provider_->Put(
      object, object_id, /*owner_address=*/rpc_address_, &object_exists));
  if (!object_exists) {
    if (pin_object) {
      // Tell the raylet to pin the object **after** it is created.
      RAY_LOG(DEBUG).WithField(object_id) << "Pinning put object";
      local_raylet_rpc_client_->PinObjectIDs(
          rpc_address_,
          {object_id},
          /*generator_id=*/ObjectID::Nil(),
          [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
            // RPC to the local raylet should never fail.
            if (!status.ok()) {
              RAY_LOG(ERROR) << "Request to local raylet to pin object failed: "
                             << status.ToString();
              return;
            }
            // Only release the object once the raylet has responded to avoid the race
            // condition that the object could be evicted before the raylet pins it.
            if (!plasma_store_provider_->Release(object_id).ok()) {
              RAY_LOG(ERROR).WithField(object_id)
                  << "Failed to release object, might cause a leak in plasma.";
            }
          });
    } else {
      RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    }
  }
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_->HasReference(object_id));
  return Status::OK();
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       const ObjectID &object_id,
                       bool pin_object) {
  RAY_RETURN_NOT_OK(WaitForActorRegistered(contained_object_ids));
  return PutInLocalPlasmaStore(object, object_id, pin_object);
}

Status CoreWorker::CreateOwnedAndIncrementLocalRef(
    bool is_experimental_mutable_object,
    const std::shared_ptr<Buffer> &metadata,
    const size_t data_size,
    const std::vector<ObjectID> &contained_object_ids,
    ObjectID *object_id,
    std::shared_ptr<Buffer> *data,
    bool inline_small_object,
    const std::optional<std::string> &tensor_transport) {
  auto status = WaitForActorRegistered(contained_object_ids);
  if (!status.ok()) {
    return status;
  }
  *object_id = ObjectID::FromIndex(worker_context_->GetCurrentInternalTaskId(),
                                   worker_context_->GetNextPutIndex());
  SubscribeToNodeChanges();
  reference_counter_->AddOwnedObject(*object_id,
                                     contained_object_ids,
                                     rpc_address_,
                                     CurrentCallSite(),
                                     data_size + metadata->Size(),
                                     LineageReconstructionEligibility::INELIGIBLE_PUT,
                                     /*add_local_ref=*/true,
                                     NodeID::FromBinary(rpc_address_.node_id()),
                                     /*tensor_transport=*/tensor_transport);

  // Register the callback to free the RDT object when it is out of scope.
  if (tensor_transport.has_value()) {
    reference_counter_->AddObjectOutOfScopeOrFreedCallback(*object_id,
                                                           free_actor_object_callback_);
  }

  status = plasma_store_provider_->Create(metadata,
                                          data_size,
                                          *object_id,
                                          /*owner_address=*/rpc_address_,
                                          data,
                                          /*created_by_worker=*/true,
                                          is_experimental_mutable_object);
  if (!status.ok()) {
    RemoveLocalReference(*object_id);
    return status;
  } else if (*data == nullptr) {
    // Object already exists in plasma. Store the in-memory value so that the
    // client will check the plasma store.
    memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                       *object_id,
                       reference_counter_->HasReference(*object_id));
  }
  return Status::OK();
}

Status CoreWorker::CreateExisting(const std::shared_ptr<Buffer> &metadata,
                                  const size_t data_size,
                                  const ObjectID &object_id,
                                  const rpc::Address &owner_address,
                                  std::shared_ptr<Buffer> *data,
                                  bool created_by_worker) {
  return plasma_store_provider_->Create(
      metadata, data_size, object_id, owner_address, data, created_by_worker);
}

Status CoreWorker::ExperimentalChannelWriteAcquire(
    const ObjectID &object_id,
    const std::shared_ptr<Buffer> &metadata,
    uint64_t data_size,
    int64_t num_readers,
    int64_t timeout_ms,
    std::shared_ptr<Buffer> *data) {
  Status status = experimental_mutable_object_provider_->GetChannelStatus(
      object_id, /*is_reader=*/false);
  if (!status.ok()) {
    return status;
  }
  return experimental_mutable_object_provider_->WriteAcquire(object_id,
                                                             data_size,
                                                             metadata->Data(),
                                                             metadata->Size(),
                                                             num_readers,
                                                             *data,
                                                             timeout_ms);
}

Status CoreWorker::ExperimentalChannelWriteRelease(const ObjectID &object_id) {
  return experimental_mutable_object_provider_->WriteRelease(object_id);
}

Status CoreWorker::ExperimentalChannelSetError(const ObjectID &object_id) {
  return experimental_mutable_object_provider_->SetError(object_id);
}

Status CoreWorker::SealOwned(const ObjectID &object_id, bool pin_object) {
  auto status = SealExisting(object_id, pin_object, ObjectID::Nil());
  if (status.ok()) {
    return status;
  }
  RemoveLocalReference(object_id);
  if (reference_counter_->HasReference(object_id)) {
    RAY_LOG(WARNING).WithField(object_id)
        << "Object failed to be put but has a nonzero ref count. This object may leak.";
  }
  return status;
}

Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object,
                                const ObjectID &generator_id,
                                const std::unique_ptr<rpc::Address> &owner_address) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    // Tell the raylet to pin the object **after** it is created.
    RAY_LOG(DEBUG).WithField(object_id) << "Pinning sealed object";
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address != nullptr ? *owner_address : rpc_address_,
        {object_id},
        generator_id,
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          // RPC to the local raylet should never fail.
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Request to local raylet to pin object failed: "
                           << status.ToString();
            return;
          }
          // Only release the object once the raylet has responded to avoid the race
          // condition that the object could be evicted before the raylet pins it.
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR).WithField(object_id)
                << "Failed to release object, might cause a leak in plasma.";
          }
        });
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    reference_counter_->FreePlasmaObjects({object_id});
  }
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_->HasReference(object_id));
  return Status::OK();
}

void CoreWorker::ExperimentalRegisterMutableObjectWriter(
    const ObjectID &writer_object_id, const std::vector<NodeID> &remote_reader_node_ids) {
  SubscribeToNodeChanges();
  {
    std::unique_lock<std::mutex> lock(gcs_client_node_cache_populated_mutex_);
    if (!gcs_client_node_cache_populated_) {
      gcs_client_node_cache_populated_cv_.wait(
          lock, [this]() { return gcs_client_node_cache_populated_; });
    }
  }
  experimental_mutable_object_provider_->RegisterWriterChannel(writer_object_id,
                                                               remote_reader_node_ids);
}

Status CoreWorker::ExperimentalRegisterMutableObjectReaderRemote(
    const ObjectID &writer_object_id,
    const std::vector<ray::experimental::ReaderRefInfo> &remote_reader_ref_info) {
  if (remote_reader_ref_info.empty()) {
    return Status::OK();
  }

  std::shared_ptr<size_t> num_replied = std::make_shared<size_t>(0);
  size_t num_requests = remote_reader_ref_info.size();
  std::promise<void> promise;
  for (const auto &reader_ref_info : remote_reader_ref_info) {
    const auto &owner_reader_actor_id = reader_ref_info.owner_reader_actor_id;
    const auto &reader_object_id = reader_ref_info.reader_ref_id;
    const auto &num_reader = reader_ref_info.num_reader_actors;
    const auto &addr = actor_task_submitter_->GetActorAddress(owner_reader_actor_id);
    // It can happen if an actor is not created yet. We assume the API is called only when
    // an actor is alive, which is true now.
    RAY_CHECK(addr.has_value());

    std::shared_ptr<rpc::CoreWorkerClientInterface> conn =
        core_worker_client_pool_->GetOrConnect(*addr);

    rpc::RegisterMutableObjectReaderRequest req;
    req.set_writer_object_id(writer_object_id.Binary());
    req.set_num_readers(num_reader);
    req.set_reader_object_id(reader_object_id.Binary());
    rpc::RegisterMutableObjectReaderReply reply;

    // TODO(sang): Add timeout.
    conn->RegisterMutableObjectReader(
        req,
        [&promise, num_replied, num_requests, addr](
            const Status &status, const rpc::RegisterMutableObjectReaderReply &) {
          RAY_CHECK_OK(status);
          *num_replied += 1;
          if (*num_replied == num_requests) {
            promise.set_value();
          }
        });
  }
  promise.get_future().wait();

  return Status::OK();
}

Status CoreWorker::ExperimentalRegisterMutableObjectReader(const ObjectID &object_id) {
  experimental_mutable_object_provider_->RegisterReaderChannel(object_id);
  return Status::OK();
}

Status CoreWorker::Get(const std::vector<ObjectID> &ids,
                       const int64_t timeout_ms,
                       std::vector<std::shared_ptr<RayObject>> &results) {
  std::unique_ptr<ScopedTaskMetricSetter> state = nullptr;
  if (options_.worker_type == WorkerType::WORKER) {
    // We track the state change only from workers.
    state = std::make_unique<ScopedTaskMetricSetter>(
        *worker_context_, task_counter_, rpc::TaskStatus::RUNNING_IN_RAY_GET);
  }
  results.resize(ids.size(), nullptr);

#if defined(__APPLE__) || defined(__linux__)
  // Check whether these are experimental.Channel objects.
  bool is_experimental_channel = false;
  for (const ObjectID &id : ids) {
    Status status =
        experimental_mutable_object_provider_->GetChannelStatus(id, /*is_reader=*/true);
    if (status.ok()) {
      is_experimental_channel = true;
      // We continue rather than break because we want to check that *all* of the
      // objects are either experimental or not experimental. We cannot have a mix of
      // the two.
      continue;
    } else if (status.IsChannelError()) {
      // The channel has been closed.
      return status;
    } else if (is_experimental_channel) {
      return Status::NotImplemented(
          "ray.get can only be called on all normal objects, or all "
          "experimental.Channel objects");
    }
  }

  // ray.get path for experimental.Channel objects.
  if (is_experimental_channel) {
    return GetExperimentalMutableObjects(ids, timeout_ms, results);
  }
#endif

  return GetObjects(ids, timeout_ms, results);
}

Status CoreWorker::GetExperimentalMutableObjects(
    const std::vector<ObjectID> &ids,
    int64_t timeout_ms,
    std::vector<std::shared_ptr<RayObject>> &results) {
  for (size_t i = 0; i < ids.size(); i++) {
    RAY_RETURN_NOT_OK(experimental_mutable_object_provider_->ReadAcquire(
        ids[i], results[i], timeout_ms));
  }
  return Status::OK();
}

Status CoreWorker::GetObjects(const std::vector<ObjectID> &ids,
                              const int64_t timeout_ms,
                              std::vector<std::shared_ptr<RayObject>> &results) {
  return GetObjectsInternal(
      ids,
      timeout_ms,
      results,
      /*allow_recovery_succession=*/true,
      /*recovery_in_progress_ids=*/nullptr);
}

Status CoreWorker::GetObjectsInternal(
    const std::vector<ObjectID> &ids,
    const int64_t timeout_ms,
    std::vector<std::shared_ptr<RayObject>> &results,
    bool allow_recovery_succession,
    const absl::flat_hash_set<ObjectID> *recovery_in_progress_ids) {
  // Normal ray.get path for immutable in-memory and shared memory objects.
  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  const auto start_time = clock_.SteadyNowMillis();

  StatusSet<StatusT::NotFound> objects_have_owners = reference_counter_->HasOwner(ids);

  if (objects_have_owners.has_error()) {
    return std::visit(
        overloaded{[](const StatusT::NotFound &not_found) {
          return Status::ObjectUnknownOwner(absl::StrFormat(
              "You are trying to access Ray objects whose owner is "
              "unknown. Please make sure that all Ray objects you are trying to access "
              "are part of the current Ray session. Note that object IDs generated "
              "randomly (ObjectID.from_random()) or out-of-band "
              "(ObjectID.from_binary(...)) cannot be passed as a task argument because "
              "Ray does not know which task created them. If this was not how your "
              "object ID was generated, please file an issue at "
              "https://github.com/ray-project/ray/issues/. %s",
              not_found.message()));
        }},
        objects_have_owners.error());
  }

  bool got_exception = false;

  if (!memory_object_ids.empty()) {
    RAY_RETURN_NOT_OK(memory_store_->Get(
        memory_object_ids, timeout_ms, *worker_context_, &result_map, &got_exception));
  }

  // Erase objects promoted to plasma from the memory-store results.
  // These requests will be retried through the plasma store provider.
  for (auto it = result_map.begin(); it != result_map.end();) {
    auto current = it++;

    if (current->second->IsInPlasmaError()) {
      RAY_LOG(DEBUG) << current->first << " in plasma, doing fetch-and-get";

      plasma_object_ids.insert(current->first);
      result_map.erase(current);
    }
  }

  if (!got_exception && !plasma_object_ids.empty()) {
    std::vector<ObjectID> object_ids(plasma_object_ids.begin(), plasma_object_ids.end());

    auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

    int64_t local_timeout_ms = timeout_ms;

    if (timeout_ms >= 0) {
      local_timeout_ms =
          std::max<int64_t>(0, timeout_ms - (clock_.SteadyNowMillis() - start_time));
    }

    RAY_LOG(DEBUG) << "Plasma GET timeout " << local_timeout_ms;

    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(
        object_ids, owner_addresses, local_timeout_ms, &result_map));
  }





  // A recovery replay may already have been accepted while an old
  // OWNER_DIED sentinel for the same deterministic ObjectID is still
  // propagating through the local object store / pull path.
  //
  // Do not expose that stale terminal value to the caller while the
  // accepted recovery attempt is still producing the replacement.
  if (recovery_in_progress_ids != nullptr &&
      !recovery_in_progress_ids->empty()) {
    std::vector<ObjectID> stale_owner_died_objects;

    for (const ObjectID &object_id : *recovery_in_progress_ids) {
      auto it = result_map.find(object_id);

      if (it == result_map.end() || it->second == nullptr) {
        continue;
      }

      rpc::ErrorType error_type;

      if (it->second->IsException(&error_type) &&
          error_type == rpc::ErrorType::OWNER_DIED) {
        stale_owner_died_objects.push_back(object_id);
      }
    }

    if (!stale_owner_died_objects.empty()) {
      // Release any plasma buffers before deleting the stale sentinel.
      for (const ObjectID &object_id : stale_owner_died_objects) {
        result_map.erase(object_id);
      }

      // Remove a possible in-memory copy.
      memory_store_->Delete(stale_owner_died_objects);

      // Remove the local plasma OWNER_DIED sentinel as well.
      absl::flat_hash_set<ObjectID> stale_plasma_ids(
          stale_owner_died_objects.begin(),
          stale_owner_died_objects.end());

      const Status delete_status =
          plasma_store_provider_->Delete(
              stale_plasma_ids,
              /*local_only=*/true);

      if (!delete_status.ok()) {
        return delete_status;
      }

      // FreeObjects is asynchronous. Wait until the stale sentinel has
      // actually disappeared before starting another pull for the same ID.
      while (true) {
        bool stale_object_present = false;

        for (const ObjectID &object_id : stale_plasma_ids) {
          bool contains = false;

          RAY_RETURN_NOT_OK(
              plasma_store_provider_->Contains(object_id, &contains));

          if (contains) {
            stale_object_present = true;
            break;
          }
        }

        if (!stale_object_present) {
          break;
        }

        const int64_t elapsed_ms =
            clock_.SteadyNowMillis() - start_time;

        if (timeout_ms >= 0 && elapsed_ms >= timeout_ms) {
          return Status::TimedOut(
              "Timed out removing stale OWNER_DIED while "
              "recovery replay was in progress.");
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(10));
      }

      RAY_LOG(INFO)
          << "Ignored stale OWNER_DIED while recovery replay "
            "is in progress";

      const int64_t elapsed_ms =
          clock_.SteadyNowMillis() - start_time;

      const int64_t remaining_timeout_ms =
          timeout_ms < 0
              ? timeout_ms
              : std::max<int64_t>(
                    0,
                    timeout_ms - elapsed_ms);

      results.assign(ids.size(), nullptr);

      return GetObjectsInternal(
          ids,
          remaining_timeout_ms,
          results,
          /*allow_recovery_succession=*/false,
          recovery_in_progress_ids);
    }
  }



  

  // OWNER_DIED can be returned either by the memory store or
  // by the plasma fetch above.
  if (allow_recovery_succession && recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr) {
    std::vector<ObjectID> recoverable_owner_died_objects;

    for (const auto &[object_id, object] : result_map) {
      if (object == nullptr) {
        continue;
      }

      rpc::ErrorType error_type;

      if (!object->IsException(&error_type) || error_type != rpc::ErrorType::OWNER_DIED) {
        continue;
      }

      RecoverySuccessionManager::BorrowedObjectRecoveryPlan plan;

      if (!recovery_succession_manager_->GetBorrowedObjectRecoveryPlan(object_id,
                                                                       &plan)) {
        RAY_LOG(WARNING).WithField(object_id) << "OWNER_DIED observed but no borrowed "
                                                 "recovery succession plan was found";
        continue;
      }

      recoverable_owner_died_objects.push_back(object_id);
    }

    if (!recoverable_owner_died_objects.empty()) {
      // Release the local plasma buffers held by result_map before
      // asking the raylet to delete the stale error objects.
      for (const ObjectID &object_id : recoverable_owner_died_objects) {
        result_map.erase(object_id);
      }

      // Delete terminal OWNER_DIED values that came directly from
      // the memory store. For objects fetched through plasma, keep
      // the existing OBJECT_IN_PLASMA marker so the retry proceeds
      // immediately to the plasma store provider.
      std::vector<ObjectID> in_memory_owner_died_objects;

      in_memory_owner_died_objects.reserve(recoverable_owner_died_objects.size());

      for (const ObjectID &object_id : recoverable_owner_died_objects) {
        if (!plasma_object_ids.contains(object_id)) {
          in_memory_owner_died_objects.push_back(object_id);
        }
      }

      if (!in_memory_owner_died_objects.empty()) {
        memory_store_->Delete(in_memory_owner_died_objects);
      }

      absl::flat_hash_set<ObjectID> plasma_owner_died_objects(
          recoverable_owner_died_objects.begin(), recoverable_owner_died_objects.end());

      // The replay uses the same ObjectID. The local terminal
      // OWNER_DIED object must be removed before replay starts,
      // otherwise the reconstructed output cannot replace it.
      const Status delete_status =
          plasma_store_provider_->Delete(plasma_owner_died_objects,
                                         /*local_only=*/true);

      if (!delete_status.ok()) {
        RAY_LOG(WARNING) << "Failed to delete local OWNER_DIED "
                            "objects before recovery succession: "
                         << delete_status;

        // Preserve the normal Ray behavior for this get.
        for (const ObjectID &object_id : recoverable_owner_died_objects) {
          result_map[object_id] = std::make_shared<RayObject>(rpc::ErrorType::OWNER_DIED);
        }
      } else {
        // FreeObjects is asynchronous. Do not start the replay until
        // the stale local OWNER_DIED object is actually absent from
        // the local plasma store. The replay produces the same
        // deterministic ObjectID, so deletion after replay starts
        // could delete the reconstructed result.
        while (true) {
          bool stale_object_present = false;

          for (const ObjectID &object_id : plasma_owner_died_objects) {
            bool contains = false;

            RAY_RETURN_NOT_OK(
                plasma_store_provider_->Contains(object_id, &contains));

            if (contains) {
              stale_object_present = true;
              break;
            }
          }

          if (!stale_object_present) {
            break;
          }

          const int64_t elapsed_ms =
              clock_.SteadyNowMillis() - start_time;

          if (timeout_ms >= 0 && elapsed_ms >= timeout_ms) {
            return Status::TimedOut(
                "Timed out removing stale local OWNER_DIED object "
                "before recovery succession replay.");
          }

          std::this_thread::sleep_for(
              std::chrono::milliseconds(10));
        }

        RAY_LOG(INFO)
            << "Confirmed stale local OWNER_DIED object removed "
               "before recovery replay";

        bool all_recoveries_accepted = true;

        for (const ObjectID &object_id : recoverable_owner_died_objects) {
          auto recovery_promise = std::make_shared<std::promise<bool>>();

          std::future<bool> recovery_future = recovery_promise->get_future();

          RAY_LOG(INFO).WithField(object_id) << "OWNER_DIED intercepted; "
                                                "trying recovery succession";

          RecoverBorrowedObject(object_id, [recovery_promise](bool accepted) {
            recovery_promise->set_value(accepted);
          });

          bool recovery_accepted = false;

          if (timeout_ms < 0) {
            recovery_accepted = recovery_future.get();
          } else {
            const int64_t elapsed_ms = clock_.SteadyNowMillis() - start_time;

            const int64_t remaining_ms = std::max<int64_t>(0, timeout_ms - elapsed_ms);

            const std::future_status wait_status =
                recovery_future.wait_for(std::chrono::milliseconds(remaining_ms));

            if (wait_status == std::future_status::ready) {
              recovery_accepted = recovery_future.get();
            }
          }

          if (!recovery_accepted) {
            all_recoveries_accepted = false;
            break;
          }
        }





        if (all_recoveries_accepted) {
          const int64_t elapsed_ms =
              clock_.SteadyNowMillis() - start_time;

          const int64_t remaining_timeout_ms =
              timeout_ms < 0
                  ? timeout_ms
                  : std::max<int64_t>(
                        0,
                        timeout_ms - elapsed_ms);

          // Only suppress stale OWNER_DIED for objects for which a
          // recovery holder actually accepted responsibility.
          absl::flat_hash_set<ObjectID> recovery_in_progress(
              recoverable_owner_died_objects.begin(),
              recoverable_owner_died_objects.end());

          results.assign(ids.size(), nullptr);

          return GetObjectsInternal(
              ids,
              remaining_timeout_ms,
              results,
              /*allow_recovery_succession=*/false,
              &recovery_in_progress);
        }

        // Recovery succession was unavailable or rejected. Return
        // the legacy OWNER_DIED result for this get.
        for (const ObjectID &object_id : recoverable_owner_died_objects) {
          result_map[object_id] = std::make_shared<RayObject>(rpc::ErrorType::OWNER_DIED);
        }
      }
    }
  }

  // Fill results in the same order as the requested object IDs.
  bool missing_result = false;
  bool will_throw_exception = false;

  for (size_t i = 0; i < ids.size(); ++i) {
    const auto pair = result_map.find(ids[i]);

    if (pair != result_map.end()) {
      results[i] = pair->second;

      RAY_CHECK(!pair->second->IsInPlasmaError());

      if (pair->second->IsException()) {
        will_throw_exception = true;
      }
    } else {
      missing_result = true;
    }
  }

  if (timeout_ms < 0 && !will_throw_exception) {
    RAY_CHECK(!missing_result);
  }

  return Status::OK();
}

Status CoreWorker::GetIfLocal(const std::vector<ObjectID> &ids,
                              std::vector<std::shared_ptr<RayObject>> *results) {
  results->resize(ids.size(), nullptr);

  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  RAY_RETURN_NOT_OK(plasma_store_provider_->GetIfLocal(ids, &result_map));
  for (size_t i = 0; i < ids.size(); i++) {
    auto pair = result_map.find(ids[i]);
    // The caller of this method should guarantee that the object exists in the plasma
    // store when this method is called.
    RAY_CHECK(pair != result_map.end());
    RAY_CHECK(pair->second != nullptr);
    (*results)[i] = pair->second;
  }
  return Status::OK();
}

Status CoreWorker::Contains(const ObjectID &object_id,
                            bool *has_object,
                            bool *is_in_plasma) {
  bool found = false;
  bool in_plasma = false;
  found = memory_store_->Contains(object_id, &in_plasma);
  if (in_plasma) {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Contains(object_id, &found));
  }
  *has_object = found;
  if (is_in_plasma != nullptr) {
    *is_in_plasma = found && in_plasma;
  }
  return Status::OK();
}

Status CoreWorker::Wait(const std::vector<ObjectID> &ids,
                        int num_objects,
                        int64_t timeout_ms,
                        std::vector<bool> *results,
                        bool fetch_local) {
  std::unique_ptr<ScopedTaskMetricSetter> state = nullptr;
  if (options_.worker_type == WorkerType::WORKER) {
    // We track the state change only from workers.
    state = std::make_unique<ScopedTaskMetricSetter>(
        *worker_context_, task_counter_, rpc::TaskStatus::RUNNING_IN_RAY_WAIT);
  }

  results->resize(ids.size(), false);

  if (num_objects <= 0 || num_objects > static_cast<int>(ids.size())) {
    return Status::Invalid(
        "Number of objects to wait for must be between 1 and the number of ids.");
  }

  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  if (memory_object_ids.size() != ids.size()) {
    return Status::Invalid("Duplicate object IDs not supported in wait.");
  }

  size_t objs_without_owners = 0;
  size_t objs_with_owners = 0;
  std::ostringstream ids_stream;

  for (size_t i = 0; i < ids.size(); i++) {
    if (!HasOwner(ids[i])) {
      ids_stream << ids[i] << " ";
      ++objs_without_owners;
    } else {
      ++objs_with_owners;
    }
    // enough owned objects to process this batch
    if (objs_with_owners == static_cast<size_t>(num_objects)) {
      break;
    }
    // not enough objects with owners to process the batch
    if (static_cast<size_t>(num_objects) > ids.size() - objs_without_owners) {
      std::ostringstream stream;
      stream << "An application is trying to access a Ray object whose owner is unknown"
             << "(" << ids_stream.str()
             << "). "
                "Please make sure that all Ray objects you are trying to access are part"
                " of the current Ray session. Note that "
                "object IDs generated randomly (ObjectID.from_random()) or out-of-band "
                "(ObjectID.from_binary(...)) cannot be passed as a task argument because"
                " Ray does not know which task created them. "
                "If this was not how your object ID was generated, please file an issue "
                "at https://github.com/ray-project/ray/issues/";
      return Status::ObjectUnknownOwner(stream.str());
    }
  }

  int64_t start_time = clock_.SteadyNowMillis();
  absl::flat_hash_set<ObjectID> ready, plasma_object_ids;
  ready.reserve(num_objects);
  RAY_RETURN_NOT_OK(memory_store_->Wait(
      memory_object_ids,
      std::min(static_cast<int>(memory_object_ids.size()), num_objects),
      timeout_ms,
      *worker_context_,
      &ready,
      &plasma_object_ids));
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);
  if (timeout_ms > 0) {
    timeout_ms = std::max(
        0, static_cast<int>(timeout_ms - (clock_.SteadyNowMillis() - start_time)));
  }
  if (fetch_local) {
    // With fetch_local we want to start fetching plasma_object_ids from other nodes'
    // plasma stores. We make the request to the plasma store even if we have
    // num_objects ready since we want to at least make the request to start pulling
    // these objects.
    if (!plasma_object_ids.empty()) {
      // Prepare object ids map
      std::vector<ObjectID> object_ids =
          std::vector<ObjectID>(plasma_object_ids.begin(), plasma_object_ids.end());
      auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

      RAY_RETURN_NOT_OK(plasma_store_provider_->Wait(
          object_ids,
          owner_addresses,
          std::min(static_cast<int>(plasma_object_ids.size()),
                   num_objects - static_cast<int>(ready.size())),
          timeout_ms,
          *worker_context_,
          &ready));
    }
  } else {
    // When we don't need to fetch_local, we don't need to wait for the objects to be
    // pulled to the local object store, so we can directly add them to the ready set.
    for (const auto &object_id : plasma_object_ids) {
      if (ready.size() == static_cast<size_t>(num_objects)) {
        break;
      }
      ready.insert(object_id);
    }
  }
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);

  for (size_t i = 0; i < ids.size(); i++) {
    if (ready.find(ids[i]) != ready.end()) {
      results->at(i) = true;
    }
  }

  return Status::OK();
}

Status CoreWorker::Delete(const std::vector<ObjectID> &object_ids, bool local_only) {
  absl::flat_hash_map<WorkerID, std::vector<ObjectID>> by_owner;
  absl::flat_hash_map<WorkerID, rpc::Address> addresses;
  // Group by owner id.
  for (const auto &obj_id : object_ids) {
    auto owner_address = GetOwnerAddressOrDie(obj_id);
    auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
    by_owner[worker_id].push_back(obj_id);
    addresses[worker_id] = owner_address;
  }
  // Send a batch delete call per owner id.
  for (const auto &entry : by_owner) {
    if (entry.first != worker_context_->GetWorkerID()) {
      RAY_LOG(INFO).WithField(entry.first)
          << "Deleting remote objects " << entry.second.size();
      auto conn = core_worker_client_pool_->GetOrConnect(addresses[entry.first]);
      rpc::DeleteObjectsRequest request;
      for (const auto &obj_id : entry.second) {
        request.add_object_ids(obj_id.Binary());
      }
      request.set_local_only(local_only);
      conn->DeleteObjects(
          request,
          [object_ids](const Status &status, const rpc::DeleteObjectsReply &reply) {
            if (status.ok()) {
              RAY_LOG(INFO) << "Completed object delete request " << status;
            } else {
              RAY_LOG(ERROR) << "Failed to delete objects, status: " << status
                             << ", object IDs: " << debug_string(object_ids);
            }
          });
    }
  }
  // Also try to delete all objects locally.
  Status status = DeleteImpl(object_ids, local_only);
  if (status.IsIOError()) {
    return Status::UnexpectedSystemExit(status.ToString());
  } else {
    return status;
  }
}

Status CoreWorker::GetLocationFromOwner(
    const std::vector<ObjectID> &object_ids,
    int64_t timeout_ms,
    std::vector<std::shared_ptr<ObjectLocation>> *results) {
  results->resize(object_ids.size());
  if (object_ids.empty()) {
    return Status::OK();
  }

  absl::flat_hash_map<rpc::Address, std::vector<ObjectID>> objects_by_owner;
  for (const auto &object_id : object_ids) {
    rpc::Address owner_address;
    RAY_RETURN_NOT_OK(GetOwnerAddress(object_id, &owner_address));
    objects_by_owner[owner_address].push_back(object_id);
  }

  auto mutex = std::make_shared<absl::Mutex>();
  auto num_remaining = std::make_shared<size_t>(0);  // Will be incremented per batch
  auto ready_promise = std::make_shared<std::promise<void>>();
  auto location_by_id =
      std::make_shared<absl::flat_hash_map<ObjectID, std::shared_ptr<ObjectLocation>>>();

  for (const auto &owner_and_objects : objects_by_owner) {
    const auto &owner_address = owner_and_objects.first;
    const auto &owner_object_ids = owner_and_objects.second;

    // Calculate the number of batches
    // Use the same config from worker_fetch_request_size
    auto batch_size =
        static_cast<size_t>(RayConfig::instance().worker_fetch_request_size());

    for (size_t batch_start = 0; batch_start < owner_object_ids.size();
         batch_start += batch_size) {
      *num_remaining += 1;
      size_t batch_end = std::min(batch_start + batch_size, owner_object_ids.size());
      auto client = core_worker_client_pool_->GetOrConnect(owner_address);
      rpc::GetObjectLocationsOwnerRequest request;
      request.set_intended_worker_id(owner_address.worker_id());

      // Add object IDs for the current batch to the request
      for (size_t i = batch_start; i < batch_end; ++i) {
        request.add_object_ids(owner_object_ids[i].Binary());
      }

      client->GetObjectLocationsOwner(
          request,
          [owner_object_ids,
           batch_start,
           mutex,
           num_remaining,
           ready_promise,
           location_by_id,
           owner_address](const Status &status,
                          const rpc::GetObjectLocationsOwnerReply &reply) {
            absl::MutexLock lock(mutex.get());
            if (status.ok()) {
              for (int i = 0; i < reply.object_location_infos_size(); ++i) {
                // Map the object ID to its location, adjusting index by batch_start
                location_by_id->emplace(
                    owner_object_ids[batch_start + i],
                    std::make_shared<ObjectLocation>(
                        CreateObjectLocation(reply.object_location_infos(i))));
              }
            } else {
              RAY_LOG(WARNING).WithField(WorkerID::FromBinary(owner_address.worker_id()))
                  << "Failed to query location information for objects "
                  << debug_string(owner_object_ids)
                  << " owned by worker with error: " << status;
            }
            (*num_remaining)--;
            if (*num_remaining == 0) {
              ready_promise->set_value();
            }
          });
    }
  }

  // Wait for all batches to be processed or timeout
  if (timeout_ms < 0) {
    ready_promise->get_future().wait();
  } else if (ready_promise->get_future().wait_for(
                 std::chrono::microseconds(timeout_ms)) != std::future_status::ready) {
    std::ostringstream stream;
    stream << "Failed querying object locations within " << timeout_ms
           << " milliseconds.";
    return Status::TimedOut(stream.str());
  }

  // Fill in the results vector
  for (size_t i = 0; i < object_ids.size(); i++) {
    auto pair = location_by_id->find(object_ids[i]);
    if (pair == location_by_id->end()) {
      continue;
    }
    (*results)[i] = pair->second;
  }

  return Status::OK();
}

void CoreWorker::TriggerGlobalGC() {
  local_raylet_rpc_client_->GlobalGC(
      [](const Status &status, const rpc::GlobalGCReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to send global GC request: " << status;
        }
      });
}

Status CoreWorker::GetPlasmaUsage(std::string &output) {
  StatusOr<std::string> response = plasma_store_provider_->GetMemoryUsage();
  if (response.ok()) {
    output = std::move(response.value());
  }
  return response.status();
}

TaskID CoreWorker::GetCallerId() const {
  TaskID caller_id;
  ActorID actor_id = GetActorId();
  if (!actor_id.IsNil()) {
    caller_id = TaskID::ForActorCreationTask(actor_id);
  } else {
    absl::MutexLock lock(&mutex_);
    caller_id = main_thread_task_id_;
  }
  return caller_id;
}

Status CoreWorker::PushError(const JobID &job_id,
                             const std::string &type,
                             const std::string &error_message,
                             double timestamp) {
  return raylet_ipc_client_->PushError(job_id, type, error_message, timestamp);
}

json CoreWorker::OverrideRuntimeEnv(const json &child,
                                    const std::shared_ptr<json> &parent) {
  // By default, the child runtime env inherits non-specified options from the
  // parent. There is one exception to this:
  //     - The env_vars dictionaries are merged, so environment variables
  //       not specified by the child are still inherited from the parent.
  json result_runtime_env = *parent;
  for (auto it = child.cbegin(); it != child.cend(); ++it) {
    if (it.key() == "env_vars" && result_runtime_env.contains("env_vars")) {
      json env_vars = it.value();
      json merged_env_vars = result_runtime_env["env_vars"];
      for (json::iterator nit = env_vars.begin(); nit != env_vars.end(); ++nit) {
        merged_env_vars[nit.key()] = nit.value();
      }
      result_runtime_env["env_vars"] = std::move(merged_env_vars);
    } else {
      result_runtime_env[it.key()] = it.value();
    }
  }
  return result_runtime_env;
}

std::shared_ptr<rpc::RuntimeEnvInfo> CoreWorker::OverrideTaskOrActorRuntimeEnvInfo(
    const std::string &serialized_runtime_env_info) const {
  auto factory = [this](const std::string &runtime_env_info_str) {
    return OverrideTaskOrActorRuntimeEnvInfoImpl(runtime_env_info_str);
  };
  return runtime_env_json_serialization_cache_.GetOrCreate(serialized_runtime_env_info,
                                                           std::move(factory));
}

std::shared_ptr<rpc::RuntimeEnvInfo> CoreWorker::OverrideTaskOrActorRuntimeEnvInfoImpl(
    const std::string &serialized_runtime_env_info) const {
  // TODO(Catch-Bull,SongGuyang): task runtime env not support the field eager_install
  // yet, we will overwrite the filed eager_install when it did.
  std::shared_ptr<json> parent = nullptr;
  std::shared_ptr<rpc::RuntimeEnvInfo> parent_runtime_env_info = nullptr;
  std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info = nullptr;
  runtime_env_info = std::make_shared<rpc::RuntimeEnvInfo>();

  if (!IsRuntimeEnvInfoEmpty(serialized_runtime_env_info)) {
    RAY_CHECK(google::protobuf::util::JsonStringToMessage(serialized_runtime_env_info,
                                                          runtime_env_info.get())
                  .ok());
  }

  if (options_.worker_type == WorkerType::DRIVER) {
    if (IsRuntimeEnvEmpty(runtime_env_info->serialized_runtime_env())) {
      return std::make_shared<rpc::RuntimeEnvInfo>(
          worker_context_->GetCurrentJobConfig().runtime_env_info());
    }

    auto job_serialized_runtime_env = worker_context_->GetCurrentJobConfig()
                                          .runtime_env_info()
                                          .serialized_runtime_env();
    if (!IsRuntimeEnvEmpty(job_serialized_runtime_env)) {
      parent = std::make_shared<json>(json::parse(job_serialized_runtime_env));
    }
    parent_runtime_env_info = std::make_shared<rpc::RuntimeEnvInfo>(
        worker_context_->GetCurrentJobConfig().runtime_env_info());
  } else {
    if (IsRuntimeEnvEmpty(runtime_env_info->serialized_runtime_env())) {
      return worker_context_->GetCurrentRuntimeEnvInfo();
    }
    parent = worker_context_->GetCurrentRuntimeEnv();
    parent_runtime_env_info = worker_context_->GetCurrentRuntimeEnvInfo();
  }
  if (parent == nullptr) {
    return runtime_env_info;
  }
  std::string serialized_runtime_env = runtime_env_info->serialized_runtime_env();
  json child_runtime_env = json::parse(serialized_runtime_env);
  auto override_runtime_env = OverrideRuntimeEnv(child_runtime_env, parent);
  auto serialized_override_runtime_env = override_runtime_env.dump();
  runtime_env_info->set_serialized_runtime_env(serialized_override_runtime_env);
  if (runtime_env_info->uris().working_dir_uri().empty() &&
      !parent_runtime_env_info->uris().working_dir_uri().empty()) {
    runtime_env_info->mutable_uris()->set_working_dir_uri(
        parent_runtime_env_info->uris().working_dir_uri());
  }
  if (runtime_env_info->uris().py_modules_uris().empty() &&
      !parent_runtime_env_info->uris().py_modules_uris().empty()) {
    runtime_env_info->mutable_uris()->clear_py_modules_uris();
    for (const std::string &uri : parent_runtime_env_info->uris().py_modules_uris()) {
      runtime_env_info->mutable_uris()->add_py_modules_uris(uri);
    }
  }

  runtime_env_json_serialization_cache_.Put(serialized_runtime_env_info,
                                            runtime_env_info);
  return runtime_env_info;
}

void CoreWorker::BuildCommonTaskSpec(
    TaskSpecBuilder &builder,
    const JobID &job_id,
    const TaskID &task_id,
    const std::string &name,
    const TaskID &current_task_id,
    uint64_t task_index,
    const TaskID &caller_id,
    const rpc::Address &address,
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    const std::string &debugger_breakpoint,
    int64_t depth,
    const std::string &serialized_runtime_env_info,
    const std::string &call_site,
    const TaskID &main_thread_current_task_id,
    const std::string &concurrency_group_name,
    bool include_job_config,
    int64_t generator_backpressure_num_objects,
    bool enable_task_events,
    const std::unordered_map<std::string, std::string> &labels,
    const LabelSelector &label_selector,
    const std::vector<FallbackOption> &fallback_strategy,
    int64_t num_objects_per_yield,
    std::vector<DeferredRecoveryFrontierGroup> *deferred_groups) {
  // Build common task spec.
  auto override_runtime_env_info =
      OverrideTaskOrActorRuntimeEnvInfo(serialized_runtime_env_info);

  bool returns_dynamic = num_returns == -1;
  if (returns_dynamic) {
    // This remote function returns 1 ObjectRef, whose value
    // is a generator of ObjectRefs.
    num_returns = 1;
  }
  // TODO(sang): Remove this and integrate it to
  // nun_returns == -1 once migrating to streaming
  // generator.
  bool is_streaming_generator = num_returns == kStreamingGeneratorReturn;
  if (is_streaming_generator) {
    num_returns = 1;
    // We are using the dynamic return if
    // the streaming generator is used.
    returns_dynamic = true;
  }
  RAY_CHECK(num_returns >= 0);
  builder.SetCommonTaskSpec(
      task_id,
      name,
      function.GetLanguage(),
      function.GetFunctionDescriptor(),
      job_id,
      include_job_config
          ? std::optional<rpc::JobConfig>(worker_context_->GetCurrentJobConfig())
          : std::optional<rpc::JobConfig>(),
      current_task_id,
      task_index,
      caller_id,
      address,
      num_returns,
      returns_dynamic,
      is_streaming_generator,
      generator_backpressure_num_objects,
      required_resources,
      required_placement_resources,
      debugger_breakpoint,
      depth,
      main_thread_current_task_id,
      call_site,
      override_runtime_env_info,
      concurrency_group_name,
      enable_task_events,
      labels,
      label_selector,
      fallback_strategy,
      num_objects_per_yield);
  // Set task arguments.
  for (const auto &arg : args) {
    builder.AddArg(*arg);
  }

  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !args.empty()) {
    EnsureRecoverySuccessionForTaskArguments(
        builder.MutableMessage(), deferred_groups);

    auto populate_argument_metadata = [this, deferred_groups](rpc::TaskSpec *message) {
      if (deferred_groups != nullptr) {
        recovery_succession_manager_
            ->PopulateTaskArgumentMetadataForDeferredFrontierDispatch(message);
      } else {
        recovery_succession_manager_->PopulateTaskArgumentMetadata(message);
      }
    };

    if (recovery_succession_profiling_enabled_) {
      const uint64_t start_ns = RecoveryProfileNowNs();
      populate_argument_metadata(builder.MutableMessage());
      recovery_succession_manager_->RecordTaskArgumentMetadataLatency(
          RecoveryProfileNowNs() - start_ns);
    } else {
      populate_argument_metadata(builder.MutableMessage());
    }
  }

}

void CoreWorker::PrestartWorkers(const std::string &serialized_runtime_env_info,
                                 uint64_t keep_alive_duration_secs,
                                 size_t num_workers) {
  rpc::PrestartWorkersRequest request;
  request.set_language(GetLanguage());
  request.set_job_id(GetCurrentJobId().Binary());
  *request.mutable_runtime_env_info() =
      *OverrideTaskOrActorRuntimeEnvInfo(serialized_runtime_env_info);
  request.set_keep_alive_duration_secs(keep_alive_duration_secs);
  request.set_num_workers(num_workers);
  local_raylet_rpc_client_->PrestartWorkers(
      request, [](const Status &status, const rpc::PrestartWorkersReply &reply) {
        if (!status.ok()) {
          RAY_LOG(INFO) << "Failed to prestart workers: " << status;
        }
      });
}

std::vector<rpc::ObjectReference> CoreWorker::SubmitTask(
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    const TaskOptions &task_options,
    int max_retries,
    bool retry_exceptions,
    const rpc::SchedulingStrategy &scheduling_strategy,
    const std::string &debugger_breakpoint,
    const std::string &serialized_retry_exception_allowlist,
    const std::string &call_site,
    const TaskID current_task_id) {
  const bool profile_normal_submit = normal_submit_stage_profiling_enabled_;
  const uint64_t normal_submit_start_ns =
      profile_normal_submit ? RecoveryProfileNowNs() : 0;

  SubscribeToNodeChanges();
  RAY_CHECK(scheduling_strategy.scheduling_strategy_case() !=
            rpc::SchedulingStrategy::SchedulingStrategyCase::SCHEDULING_STRATEGY_NOT_SET);

  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_->GetNextTaskIndex();
  const auto task_id = TaskID::ForNormalTask(worker_context_->GetCurrentJobID(),
                                             worker_context_->GetCurrentInternalTaskId(),
                                             next_task_index);
  auto constrained_resources =
      AddPlacementGroupConstraint(task_options.resources, scheduling_strategy);

  auto task_name = task_options.name.empty()
                       ? function.GetFunctionDescriptor()->DefaultTaskName()
                       : task_options.name;
  int64_t depth = worker_context_->GetTaskDepth() + 1;

  const bool defer_recovery_frontier_dispatch =
      recovery_succession_enabled_ &&
      recovery_witness_holder_baseline_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      recovery_succession_manager_->RecoveryFrontierEnabled() &&
      RayConfig::instance().recovery_frontier_group_size() > 1;
  std::vector<DeferredRecoveryFrontierGroup> deferred_recovery_frontier_groups;

  // TODO(ekl) offload task building onto a thread pool for performance

  const uint64_t normal_submit_prebuild_done_ns =
      profile_normal_submit ? RecoveryProfileNowNs() : 0;

  BuildCommonTaskSpec(builder,
                      worker_context_->GetCurrentJobID(),
                      task_id,
                      task_name,
                      current_task_id != TaskID::Nil()
                          ? current_task_id
                          : worker_context_->GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      task_options.num_returns,
                      constrained_resources,
                      constrained_resources,
                      debugger_breakpoint,
                      depth,
                      task_options.serialized_runtime_env_info,
                      call_site,
                      worker_context_->GetMainThreadOrActorCreationTaskID(),
                      /*concurrency_group_name=*/"",
                      /*include_job_config=*/true,
                      /*generator_backpressure_num_objects=*/
                      task_options.generator_backpressure_num_objects,
                      /*enable_task_events=*/task_options.enable_task_events,
                      task_options.labels,
                      task_options.label_selector,
                      task_options.fallback_strategy,
                      task_options.num_objects_per_yield,
                      defer_recovery_frontier_dispatch
                          ? &deferred_recovery_frontier_groups
                          : nullptr);
  const uint64_t normal_submit_build_common_done_ns =
      profile_normal_submit ? RecoveryProfileNowNs() : 0;

  ActorID root_detached_actor_id;
  if (!worker_context_->GetRootDetachedActorID().IsNil()) {
    root_detached_actor_id = worker_context_->GetRootDetachedActorID();
  }
  builder.SetNormalTaskSpec(max_retries,
                            retry_exceptions,
                            serialized_retry_exception_allowlist,
                            scheduling_strategy,
                            root_detached_actor_id);

  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
  RAY_LOG(DEBUG) << "Submitting normal task " << task_spec.DebugString();
  const uint64_t normal_submit_finalize_done_ns =
      profile_normal_submit ? RecoveryProfileNowNs() : 0;

  std::vector<rpc::ObjectReference> returned_refs;
  returned_refs = task_manager_->AddPendingTask(
      task_spec.CallerAddress(), task_spec, CurrentCallSite(), max_retries);
  const uint64_t normal_submit_add_pending_done_ns =
      profile_normal_submit ? RecoveryProfileNowNs() : 0;

  // Patch 4L: retain one correctness-preserving owner TaskSpec copy for
  // eligible lazy-recovery tasks. This does NOT activate recovery: no manifest,
  // witness, candidate, holder, or control RPC is created here.
  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !task_spec.GetMessage().has_recovery_manifest() &&
      RecoverySuccessionManager::IsEligibleTask(task_spec.GetMessage())) {
    // TaskManager already owns this immutable TaskSpec. Pin that existing
    // entry rather than maintaining a second dormant protobuf in adaptive
    // Recovery Succession. Fixed-R already used this same TaskManager pin.
    RAY_CHECK(task_manager_->PinTaskForRecoverySuccession(task_spec.TaskId()))
        << "Eligible recovery task disappeared before TaskManager pin: "
        << task_spec.TaskId();

    recovery_succession_manager_->RetainOwnerTaskSpecForLazyRecovery(
        task_spec, returned_refs, /*task_manager_owns_recipe=*/true);

    // Fixed-R retains the old per-return callback lifetime path. Adaptive
    // Succession relies on TaskManager's existing lineage-release signal instead.
    if (recovery_witness_holder_baseline_enabled_) {
      auto on_owner_return_deleted = [this](const ObjectID &deleted_object_id) {
        if (!recovery_succession_enabled_ ||
            recovery_succession_manager_ == nullptr) {
          return;
        }

        const TaskID deleted_task_id = deleted_object_id.TaskId();

        bool final_return_deleted = false;
        const bool should_tombstone =
            recovery_succession_manager_->HandleOwnerReturnRefDeleted(
                deleted_object_id, &final_return_deleted);

        if (final_return_deleted) {
          task_manager_->ReleaseTaskForRecoverySuccession(deleted_task_id);
        }

        if (!should_tombstone) {
          return;
        }

        io_service_.post(
            [this, deleted_task_id] {
              if (!recovery_succession_enabled_ ||
                  recovery_succession_manager_ == nullptr) {
                return;
              }

              auto tombstone =
                  recovery_succession_manager_->BuildTombstoneForTask(deleted_task_id);
              if (!tombstone.has_value()) {
                return;
              }

              const TaskID tombstone_task_id =
                  TaskID::FromBinary(tombstone->task_id());
              if (!recovery_tombstones_in_flight_.insert(tombstone_task_id).second) {
                return;
              }

              RAY_LOG(INFO).WithField(tombstone_task_id)
                  << "Owner return refs released; publishing recovery tombstone";

              PublishRecoveryTombstone(std::move(tombstone.value()));
            },
            "CoreWorker.PublishRecoveryTombstone");
      };

      for (const rpc::ObjectReference &returned_ref : returned_refs) {
        if (returned_ref.object_id().size() != ObjectID::Size()) {
          continue;
        }

        const ObjectID object_id =
            ObjectID::FromBinary(returned_ref.object_id());

        const bool callback_added =
            reference_counter_->AddObjectRefDeletedCallback(
                object_id, on_owner_return_deleted);

        if (!callback_added) {
          on_owner_return_deleted(object_id);
        }
      }    }

  }

  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      task_spec.GetMessage().has_recovery_manifest()) {

    if (recovery_succession_profiling_enabled_) {
      const uint64_t start_ns = RecoveryProfileNowNs();

      recovery_succession_manager_->RegisterOwnedTask(
          task_spec,
          &returned_refs);

      recovery_succession_manager_->RecordRegisterOwnedTaskLatency(
          RecoveryProfileNowNs() - start_ns);
    } else {
      recovery_succession_manager_->RegisterOwnedTask(
          task_spec,
          &returned_refs);
    }
  }

  const uint64_t normal_submit_owner_setup_done_ns =
      profile_normal_submit ? RecoveryProfileNowNs() : 0;

  if (defer_recovery_frontier_dispatch &&
      !deferred_recovery_frontier_groups.empty()) {
    const TaskID deferred_task_id = task_spec.TaskId();
    std::vector<DeferredRecoveryFrontierGroup> groups_to_start;

    {
      std::lock_guard<std::mutex> lock(recovery_frontier_deferred_task_mutex_);
      auto [task_it, task_inserted] =
          recovery_frontier_deferred_tasks_.try_emplace(deferred_task_id);
      RAY_CHECK(task_inserted)
          << "Duplicate deferred Recovery Frontier task state for "
          << deferred_task_id;

      task_it->second.remaining_groups =
          deferred_recovery_frontier_groups.size();
      task_it->second.task_to_dispatch.emplace(std::move(task_spec));

      groups_to_start.reserve(deferred_recovery_frontier_groups.size());
      for (const DeferredRecoveryFrontierGroup &group :
           deferred_recovery_frontier_groups) {
        auto [group_it, group_inserted] =
            recovery_frontier_deferred_group_waiters_.try_emplace(
                group.group_id);
        group_it->second.push_back(deferred_task_id);

        // Only the first waiter starts the group's asynchronous publication.
        // Later tasks merely join the waiter vector and add no callbacks,
        // timers, atomics, or per-task readiness objects.
        if (group_inserted) {
          groups_to_start.push_back(group);
        }
      }
    }

    for (const DeferredRecoveryFrontierGroup &group : groups_to_start) {
      PublishRecoveryFrontierGroupAsync(
          group.group_id,
          group.protection_manifest,
          [this, group_id = group.group_id]() {
            CompleteDeferredRecoveryFrontierGroup(group_id);
          });
    }
  } else {
    io_service_.post(
        [this, task_spec = std::move(task_spec)]() mutable {
          normal_task_submitter_->SubmitTask(std::move(task_spec));
        },
        "CoreWorker.SubmitTask");
  }

  if (profile_normal_submit) {
    const uint64_t normal_submit_end_ns = RecoveryProfileNowNs();
    normal_submit_profile_calls_.fetch_add(1, std::memory_order_relaxed);
    normal_submit_prebuild_time_ns_.fetch_add(
        normal_submit_prebuild_done_ns - normal_submit_start_ns,
        std::memory_order_relaxed);
    normal_submit_build_common_time_ns_.fetch_add(
        normal_submit_build_common_done_ns - normal_submit_prebuild_done_ns,
        std::memory_order_relaxed);
    normal_submit_finalize_spec_time_ns_.fetch_add(
        normal_submit_finalize_done_ns - normal_submit_build_common_done_ns,
        std::memory_order_relaxed);
    normal_submit_add_pending_time_ns_.fetch_add(
        normal_submit_add_pending_done_ns - normal_submit_finalize_done_ns,
        std::memory_order_relaxed);
    normal_submit_owner_setup_time_ns_.fetch_add(
        normal_submit_owner_setup_done_ns - normal_submit_add_pending_done_ns,
        std::memory_order_relaxed);
    normal_submit_dispatch_setup_time_ns_.fetch_add(
        normal_submit_end_ns - normal_submit_owner_setup_done_ns,
        std::memory_order_relaxed);
    normal_submit_total_time_ns_.fetch_add(
        normal_submit_end_ns - normal_submit_start_ns,
        std::memory_order_relaxed);
  }

  return returned_refs;
}

Status CoreWorker::CreateActor(const RayFunction &function,
                               const std::vector<std::unique_ptr<TaskArg>> &args,
                               const ActorCreationOptions &actor_creation_options,
                               const std::string &extension_data,
                               const std::string &call_site,
                               ActorID *return_actor_id) {
  SubscribeToNodeChanges();
  RAY_CHECK(actor_creation_options.scheduling_strategy.scheduling_strategy_case() !=
            rpc::SchedulingStrategy::SchedulingStrategyCase::SCHEDULING_STRATEGY_NOT_SET);

  bool is_detached = false;
  if (!actor_creation_options.is_detached.has_value()) {
    /// Since this actor doesn't have a specified lifetime on creation, let's use
    /// the default value of the job.
    is_detached = worker_context_->GetCurrentJobConfig().default_actor_lifetime() ==
                  ray::rpc::JobConfig_ActorLifetime_DETACHED;
  } else {
    is_detached = actor_creation_options.is_detached.value();
  }

  const auto next_task_index = worker_context_->GetNextTaskIndex();
  const ActorID actor_id = ActorID::Of(worker_context_->GetCurrentJobID(),
                                       worker_context_->GetCurrentTaskID(),
                                       next_task_index);
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const JobID job_id = worker_context_->GetCurrentJobID();
  // Propagate existing environment variable overrides, but override them with any new
  // ones
  TaskSpecBuilder builder;
  auto new_placement_resources =
      AddPlacementGroupConstraint(actor_creation_options.placement_resources,
                                  actor_creation_options.scheduling_strategy);
  auto new_resource = AddPlacementGroupConstraint(
      actor_creation_options.resources, actor_creation_options.scheduling_strategy);
  const auto actor_name = actor_creation_options.name;
  const auto task_name =
      actor_name.empty()
          ? function.GetFunctionDescriptor()->DefaultTaskName()
          : actor_name + ":" + function.GetFunctionDescriptor()->CallString();
  int64_t depth = worker_context_->GetTaskDepth() + 1;
  BuildCommonTaskSpec(builder,
                      job_id,
                      actor_creation_task_id,
                      task_name,
                      worker_context_->GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      /*num_returns=*/0,
                      new_resource,
                      new_placement_resources,
                      /*debugger_breakpoint=*/"",
                      depth,
                      actor_creation_options.serialized_runtime_env_info,
                      call_site,
                      worker_context_->GetMainThreadOrActorCreationTaskID(),
                      /*concurrency_group_name=*/"",
                      /*include_job_config=*/true,
                      /*generator_backpressure_num_objects=*/-1,
                      /*enable_task_events=*/actor_creation_options.enable_task_events,
                      actor_creation_options.labels,
                      actor_creation_options.label_selector,
                      actor_creation_options.fallback_strategy);

  // If the namespace is not specified, get it from the job.
  const auto ray_namespace = (actor_creation_options.ray_namespace.empty()
                                  ? worker_context_->GetCurrentJobConfig().ray_namespace()
                                  : actor_creation_options.ray_namespace);
  auto actor_handle = std::make_unique<ActorHandle>(
      actor_id,
      GetCallerId(),
      rpc_address_,
      job_id,
      /*actor_cursor=*/ObjectID::FromIndex(actor_creation_task_id, 1),
      function.GetLanguage(),
      function.GetFunctionDescriptor(),
      extension_data,
      actor_creation_options.max_task_retries,
      actor_name,
      ray_namespace,
      actor_creation_options.max_pending_calls,
      actor_creation_options.allow_out_of_order_execution,
      actor_creation_options.enable_tensor_transport,
      actor_creation_options.enable_task_events,
      actor_creation_options.labels,
      is_detached,
      actor_creation_options.actor_generator_backpressure_num_objects);
  std::string serialized_actor_handle;
  actor_handle->Serialize(&serialized_actor_handle);
  ActorID root_detached_actor_id;
  if (is_detached) {
    root_detached_actor_id = actor_id;
  } else if (!worker_context_->GetRootDetachedActorID().IsNil()) {
    root_detached_actor_id = worker_context_->GetRootDetachedActorID();
  }
  builder.SetActorCreationTaskSpec(
      actor_id,
      serialized_actor_handle,
      actor_creation_options.scheduling_strategy,
      actor_creation_options.max_restarts,
      actor_creation_options.max_task_retries,
      actor_creation_options.dynamic_worker_options,
      actor_creation_options.max_concurrency,
      is_detached,
      actor_name,
      ray_namespace,
      actor_creation_options.is_asyncio,
      actor_creation_options.concurrency_groups,
      extension_data,
      actor_creation_options.allow_out_of_order_execution,
      root_detached_actor_id,
      actor_creation_options.actor_generator_backpressure_num_objects);
  // Add the actor handle before we submit the actor creation task, since the
  // actor handle must be in scope by the time the GCS sends the
  // WaitForActorRefDeletedRequest.
  RAY_CHECK(actor_manager_->EmplaceNewActorHandle(
      std::move(actor_handle), CurrentCallSite(), rpc_address_, /*owned=*/!is_detached))
      << "Attempt to emplace new actor handle for the actor being created with actor "
         "id: "
      << actor_id
      << " failed because an actor handle with the same actor id has already been "
         "added";
  *return_actor_id = actor_id;
  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
  RAY_LOG(DEBUG) << "Submitting actor creation task " << task_spec.DebugString();

  auto ref_is_detached_actor = [this](const std::string &object_id) {
    auto ref_object_id = ObjectID::FromBinary(object_id);
    if (ObjectID::IsActorID(ref_object_id)) {
      auto ref_actor_id = ObjectID::ToActorID(ref_object_id);
      if (auto ref_actor_handle = actor_manager_->GetActorHandleIfExists(ref_actor_id)) {
        if (ref_actor_handle->IsDetached()) {
          return true;
        }
      }
    }
    return false;
  };
  if (task_spec.MaxActorRestarts() != 0) {
    bool actor_restart_warning = false;
    for (size_t i = 0; i < task_spec.NumArgs(); i++) {
      if (task_spec.ArgByRef(i)) {
        actor_restart_warning = true;
        break;
      }
      if (!task_spec.ArgInlinedRefs(i).empty()) {
        for (const auto &ref : task_spec.ArgInlinedRefs(i)) {
          if (!ref_is_detached_actor(ref.object_id())) {
            // There's an inlined ref that's not a detached actor, so we want to
            // show the warning.
            actor_restart_warning = true;
            break;
          }
        }
      }
      if (actor_restart_warning) {
        break;
      }
    }
    if (actor_restart_warning) {
      RAY_LOG_ONCE_PER_PROCESS(ERROR)
          << "Actor " << (actor_name.empty() ? "" : (actor_name + " "))
          << "with class name: '" << function.GetFunctionDescriptor()->ClassName()
          << "' and ID: '" << task_spec.ActorCreationId()
          << "' has constructor arguments in the object store and max_restarts > 0. If "
             "the arguments in the object store go out of scope or are lost, the "
             "actor restart will fail. See "
             "https://github.com/ray-project/ray/issues/53727 for more details.";
    }
  }

  task_manager_->AddPendingTask(
      rpc_address_,
      task_spec,
      CurrentCallSite(),
      // Actor creation task retry happens on GCS not on core worker.
      /*max_retries=*/0);

  if (actor_name.empty()) {
    io_service_.post(
        [this, task_spec = std::move(task_spec)]() {
          actor_creator_->AsyncRegisterActor(task_spec, [this, task_spec](Status status) {
            if (!status.ok()) {
              RAY_LOG(ERROR).WithField(task_spec.ActorCreationId())
                  << "Failed to register actor. Error message: " << status;
              task_manager_->FailPendingTask(
                  task_spec.TaskId(), rpc::ErrorType::ACTOR_CREATION_FAILED, &status);
            } else {
              actor_task_submitter_->SubmitActorCreationTask(task_spec);
            }
          });
        },
        "ActorCreator.AsyncRegisterActor");
  } else {
    // For named actor, we still go through the sync way because for
    // functions like list actors these actors need to be there, especially
    // for local driver. But the current code all go through the gcs right now.
    auto status = actor_creator_->RegisterActor(task_spec);
    if (!status.ok()) {
      task_manager_->FailPendingTask(
          task_spec.TaskId(), rpc::ErrorType::ACTOR_CREATION_FAILED, &status);
      // Detached actor doesn't need ref counting.
      if (!is_detached) {
        RemoveActorHandleReference(actor_id);
      }
      return status;
    }
    io_service_.post(
        [this, task_spec = std::move(task_spec)]() {
          actor_task_submitter_->SubmitActorCreationTask(task_spec);
        },
        "CoreWorker.SubmitTask");
  }
  return Status::OK();
}

Status CoreWorker::CreatePlacementGroup(
    const PlacementGroupCreationOptions &placement_group_creation_options,
    PlacementGroupID *return_placement_group_id) {
  const auto &bundles = placement_group_creation_options.bundles_;
  for (const auto &bundle : bundles) {
    for (const auto &resource : bundle) {
      if (resource.first == kBundle_ResourceLabel) {
        std::ostringstream stream;
        stream << kBundle_ResourceLabel << " is a system reserved resource, which is not "
               << "allowed to be used in placement group. ";
        return Status::Invalid(stream.str());
      }
    }
  }
  const PlacementGroupID placement_group_id = PlacementGroupID::Of(GetCurrentJobId());
  PlacementGroupSpecBuilder builder;
  builder.SetPlacementGroupSpec(placement_group_id,
                                placement_group_creation_options.name_,
                                placement_group_creation_options.bundles_,
                                placement_group_creation_options.strategy_,
                                placement_group_creation_options.is_detached_,
                                placement_group_creation_options.soft_target_node_id_,
                                worker_context_->GetCurrentJobID(),
                                worker_context_->GetCurrentActorID(),
                                worker_context_->CurrentActorDetached(),
                                placement_group_creation_options.bundle_label_selector_,
                                placement_group_creation_options.topology_strategy_);
  PlacementGroupSpecification placement_group_spec = builder.Build();
  *return_placement_group_id = placement_group_id;
  RAY_LOG(INFO).WithField(placement_group_id)
      << "Submitting Placement Group creation to GCS";
  auto status =
      gcs_client_->PlacementGroups().SyncCreatePlacementGroup(placement_group_spec);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in creating the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status;
}

Status CoreWorker::RemovePlacementGroup(const PlacementGroupID &placement_group_id) {
  // Synchronously wait for placement group removal.
  auto status =
      gcs_client_->PlacementGroups().SyncRemovePlacementGroup(placement_group_id);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in removing the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status;
}

Status CoreWorker::WaitPlacementGroupReady(const PlacementGroupID &placement_group_id,
                                           int64_t timeout_seconds) {
  auto status = gcs_client_->PlacementGroups().SyncWaitUntilReady(placement_group_id,
                                                                  timeout_seconds);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in waiting for placement group " << placement_group_id
           << " creation.";
    return Status::TimedOut(stream.str());
  }
  return status;
}

ObjectID CoreWorker::AsyncWaitPlacementGroupReady(
    const PlacementGroupID &placement_group_id,
    const std::string &serialized_object_data,
    const std::string &serialized_object_metadata) {
  // Generate ObjectID and register ownership.
  // The object will be stored directly in memory_store_ and fate-shares with the owner,
  // so we set pinned_at_node_id to nullopt (same as small task returns).
  ObjectID object_id = ObjectID::FromIndex(worker_context_->GetCurrentInternalTaskId(),
                                           worker_context_->GetNextPutIndex());
  reference_counter_->AddOwnedObject(object_id,
                                     /*contained_object_ids=*/{},
                                     rpc_address_,
                                     CurrentCallSite(),
                                     /*object_size=*/-1,
                                     LineageReconstructionEligibility::INELIGIBLE_PUT,
                                     /*add_local_ref=*/true,
                                     /*pinned_at_node_id=*/std::nullopt);

  // Async RPC to GCS that returns when the placement group is ready (or removed).
  // The callback puts the result into memory store, completing ray.get()/wait()/await.
  rpc::WaitPlacementGroupUntilReadyRequest request;
  request.set_placement_group_id(placement_group_id.Binary());

  gcs_client_->GetGcsRpcClient().WaitPlacementGroupUntilReady(
      std::move(request),
      [this, object_id, serialized_object_data, serialized_object_metadata](
          const Status &status, const rpc::WaitPlacementGroupUntilReadyReply &reply) {
        // timeout_ms=-1 retries transient gRPC failures, so any other error
        // here indicates an unexpected GCS server-side failure.
        RAY_CHECK(status.ok() || status.IsNotFound())
            << "Unexpected status from WaitPlacementGroupUntilReady: " << status;

        std::shared_ptr<RayObject> result;
        if (status.ok()) {
          auto data = std::make_shared<LocalMemoryBuffer>(serialized_object_data.size());
          memcpy(
              data->Data(), serialized_object_data.data(), serialized_object_data.size());
          auto metadata =
              std::make_shared<LocalMemoryBuffer>(serialized_object_metadata.size());
          memcpy(metadata->Data(),
                 serialized_object_metadata.data(),
                 serialized_object_metadata.size());
          result = std::make_shared<RayObject>(
              data, metadata, std::vector<rpc::ObjectReference>());
        } else {
          result =
              std::make_shared<RayObject>(rpc::ErrorType::TASK_PLACEMENT_GROUP_REMOVED);
        }
        memory_store_->Put(*result, object_id, /*has_reference=*/true);
      },
      // timeout_ms=-1 means infinite wait with automatic retry.
      // Users can still set their own timeout via ray.get(ref, timeout=...).
      /*timeout_ms=*/-1);

  return object_id;
}

Status CoreWorker::SubmitActorTask(
    const ActorID &actor_id,
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    const TaskOptions &task_options,
    int max_retries,
    bool retry_exceptions,
    const std::string &serialized_retry_exception_allowlist,
    const std::string &call_site,
    std::vector<rpc::ObjectReference> &task_returns,
    const TaskID current_task_id) {
  SubscribeToNodeChanges();
  absl::ReleasableMutexLock lock(&actor_task_mutex_);
  task_returns.clear();
  if (!actor_task_submitter_->CheckActorExists(actor_id)) {
    std::string err_msg = absl::StrFormat(
        "Can't find actor %s. It might be dead or it's from a different cluster",
        actor_id.Hex());
    return Status::NotFound(err_msg);
  }
  /// Check whether backpressure may happen at the very beginning of submitting a task.
  if (actor_task_submitter_->PendingTasksFull(actor_id)) {
    RAY_LOG(DEBUG).WithField(actor_id)
        << "Back pressure occurred while submitting the actor task. "
        << actor_task_submitter_->DebugString(actor_id);
    return Status::OutOfResource(absl::StrFormat(
        "Too many tasks (%d) pending to be executed for actor %s. Please try later",
        actor_task_submitter_->NumPendingTasks(actor_id),
        actor_id.Hex()));
  }

  auto actor_handle = actor_manager_->GetActorHandle(actor_id);
  // Subscribe the actor state when we first submit the actor task. It is to reduce the
  // number of connections. The method is idempotent.
  actor_manager_->SubscribeActorState(actor_id);

  // Build common task spec.
  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_->GetNextTaskIndex();
  const TaskID actor_task_id =
      TaskID::ForActorTask(worker_context_->GetCurrentJobID(),
                           worker_context_->GetCurrentInternalTaskId(),
                           next_task_index,
                           actor_handle->GetActorID());
  const std::unordered_map<std::string, double> required_resources;
  const auto task_name = task_options.name.empty()
                             ? function.GetFunctionDescriptor()->DefaultTaskName()
                             : task_options.name;

  const bool defer_recovery_frontier_dispatch =
      recovery_succession_enabled_ &&
      recovery_witness_holder_baseline_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      recovery_succession_manager_->RecoveryFrontierEnabled() &&
      RayConfig::instance().recovery_frontier_group_size() > 1;
  std::vector<DeferredRecoveryFrontierGroup> deferred_recovery_frontier_groups;

  // The depth of the actor task is depth of the caller + 1
  // The caller is not necessarily the creator of the actor.
  int64_t depth = worker_context_->GetTaskDepth() + 1;
  BuildCommonTaskSpec(builder,
                      actor_handle->CreationJobID(),
                      actor_task_id,
                      task_name,
                      current_task_id != TaskID::Nil()
                          ? current_task_id
                          : worker_context_->GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      task_options.num_returns,
                      task_options.resources,
                      required_resources,
                      /*debugger_breakpoint=*/"",
                      depth,
                      /*serialized_runtime_env_info=*/"{}",
                      call_site,
                      worker_context_->GetMainThreadOrActorCreationTaskID(),
                      task_options.concurrency_group_name,
                      /*include_job_config=*/false,
                      /*generator_backpressure_num_objects=*/
                      task_options.generator_backpressure_num_objects,
                      /*enable_task_events=*/task_options.enable_task_events,
                      /*labels=*/task_options.labels,
                      /*label_selector=*/{},
                      /*fallback_strategy=*/{},
                      task_options.num_objects_per_yield,
                      defer_recovery_frontier_dispatch
                          ? &deferred_recovery_frontier_groups
                          : nullptr);
  // NOTE: placement_group_capture_child_tasks and runtime_env will
  // be ignored in the actor because we should always follow the actor's option.

  actor_handle->SetActorTaskSpec(builder,
                                 ObjectID::Nil(),
                                 max_retries,
                                 retry_exceptions,
                                 serialized_retry_exception_allowlist,
                                 task_options.concurrency_group_name,
                                 task_options.tensor_transport);
  // Submit task.
  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
  RAY_LOG(DEBUG) << "Submitting actor task " << task_spec.DebugString();
  task_returns = task_manager_->AddPendingTask(
      rpc_address_, task_spec, CurrentCallSite(), max_retries);

  const bool defer_actor_dependency_resolution =
      defer_recovery_frontier_dispatch &&
      !deferred_recovery_frontier_groups.empty();
  // Reserve the actor sequence position immediately. When deferred, the
  // ActorTaskSubmitter holds dependency resolution until all Frontier groups
  // protecting this task's arguments have committed.
  actor_task_submitter_->SubmitTask(task_spec, defer_actor_dependency_resolution);

  if (defer_actor_dependency_resolution) {
    auto remaining_groups = std::make_shared<std::atomic<size_t>>(
        deferred_recovery_frontier_groups.size());
    for (const DeferredRecoveryFrontierGroup &group :
         deferred_recovery_frontier_groups) {
      PublishRecoveryFrontierGroupAsync(
          group.group_id,
          group.protection_manifest,
          [this, actor_task_id, remaining_groups]() {
            if (remaining_groups->fetch_sub(1, std::memory_order_acq_rel) == 1) {
              actor_task_submitter_->ResumeDeferredTask(actor_task_id);
            }
          });
    }
  }

  return Status::OK();
}

Status CoreWorker::CancelTask(const ObjectID &object_id,
                              bool force_kill,
                              bool recursive) {
  rpc::Address obj_addr;
  if (!reference_counter_->GetOwner(object_id, &obj_addr)) {
    return Status::Invalid("No owner found for object.");
  }

  if (obj_addr.SerializeAsString() != rpc_address_.SerializeAsString()) {
    // We don't have RequestOwnerToCancelTask for actor_task_submitter_
    // because it requires the same implementation.
    RAY_LOG(DEBUG).WithField(object_id)
        << "Request to cancel a task of object to an owner "
        << obj_addr.SerializeAsString();
    normal_task_submitter_->RequestOwnerToCancelTask(
        object_id, obj_addr, force_kill, recursive);
    return Status::OK();
  }

  auto task_spec = task_manager_->GetTaskSpec(object_id.TaskId());
  if (!task_spec.has_value()) {
    // Task is already finished.
    RAY_LOG(DEBUG).WithField(object_id)
        << "Cancel request is ignored because the task is already canceled "
           "for an object";
    return Status::OK();
  }

  if (task_spec.value().IsActorCreationTask()) {
    RAY_LOG(FATAL) << "Cannot cancel actor creation tasks";
  }

  if (task_spec->IsActorTask()) {
    if (force_kill) {
      return Status::InvalidArgument("force=True is not supported for actor tasks.");
    }

    actor_task_submitter_->CancelTask(task_spec.value(), recursive);
  } else {
    normal_task_submitter_->CancelTask(task_spec.value(), force_kill, recursive);
  }
  return Status::OK();
}

bool CoreWorker::IsTaskCanceled(const TaskID &task_id) const {
  // Check if the task is canceled on executor side. Check the canceled_tasks_ which is
  // populated when CancelTask RPC is received.
  absl::MutexLock lock(&mutex_);
  return canceled_tasks_.find(task_id) != canceled_tasks_.end();
}

bool CoreWorker::AddObjectOutOfScopeOrFreedCallback(
    const ObjectID &object_id, const std::function<void(const ObjectID &)> &callback) {
  auto wrapped = [&object_freed_callback_service = object_freed_callback_service_,
                  callback](const ObjectID &id) {
    object_freed_callback_service.post([callback, id]() { callback(id); },
                                       "CoreWorker.ObjFreedCb");
  };
  return reference_counter_->AddObjectOutOfScopeOrFreedCallback(object_id, wrapped);
}

bool CoreWorker::AddObjectOutOfScopeOrFreedCallback(const ObjectID &object_id,
                                                    void (*callback)(const ObjectID &,
                                                                     void *),
                                                    void *callback_context) {
  RAY_CHECK(callback != nullptr) << "callback must not be null";
  return AddObjectOutOfScopeOrFreedCallback(
      object_id, [callback, callback_context](const ObjectID &id) {
        callback(id, callback_context);
      });
}

Status CoreWorker::CheckObjectOwnedByUs(const ObjectID &object_id) const {
  if (reference_counter_->OwnedByUs(object_id)) {
    return Status::OK();
  }
  return Status::InvalidArgument(absl::StrFormat(
      "Cannot register an out-of-scope/freed callback for object %s: it is not "
      "owned by this worker (it may be owned by another worker, or have no "
      "ownership record). These callbacks can only be registered by the owner.",
      object_id.Hex()));
}

bool CoreWorker::ShouldInterruptTaskForCancellation() const {
  if (worker_context_->GetCurrentJobID().IsNil()) {
    return false;
  }
  const TaskID &task_id = worker_context_->GetCurrentTaskID();
  if (task_id.IsNil()) {
    return false;
  }
  return IsTaskCanceled(task_id);
}

Status CoreWorker::CancelChildren(const TaskID &task_id, bool force_kill) {
  absl::flat_hash_set<TaskID> unknown_child_task_ids;
  auto child_task_ids = task_manager_->GetPendingChildrenTasks(task_id);
  for (const auto &child_id : child_task_ids) {
    auto child_spec = task_manager_->GetTaskSpec(child_id);
    if (!child_spec.has_value()) {
      unknown_child_task_ids.insert(child_id);
    } else if (child_spec->IsActorTask()) {
      actor_task_submitter_->CancelTask(std::move(*child_spec), true);
    } else {
      normal_task_submitter_->CancelTask(std::move(*child_spec), force_kill, true);
    }
  }

  if (unknown_child_task_ids.empty()) {
    return Status::OK();
  }

  constexpr size_t kMaxFailedTaskSampleSize = 10;
  std::ostringstream ostr;
  ostr << "Failed to cancel all the children tasks of " << task_id << " recursively.\n"
       << "Here are up to " << kMaxFailedTaskSampleSize
       << " samples tasks that failed to be canceled\n";
  const auto failure_status_str =
      Status::UnknownError("Recursive task cancellation failed--check warning logs.")
          .ToString();
  size_t failures = 0;
  for (const auto &child_id : unknown_child_task_ids) {
    ostr << "\t" << child_id << ", " << failure_status_str << "\n";
    failures += 1;
    if (failures >= kMaxFailedTaskSampleSize) {
      break;
    }
  }
  ostr << "Total Recursive cancelation success: "
       << (child_task_ids.size() - unknown_child_task_ids.size())
       << ", failures: " << unknown_child_task_ids.size();
  return Status::UnknownError(ostr.str());
}

Status CoreWorker::KillActor(const ActorID &actor_id, bool force_kill, bool no_restart) {
  std::promise<Status> p;
  auto f = p.get_future();
  io_service_.post(
      [this, p = &p, actor_id, force_kill, no_restart]() {
        auto cb = [this, p, actor_id, force_kill, no_restart](Status status) mutable {
          if (status.ok()) {
            gcs_client_->Actors().AsyncKillActor(
                actor_id, force_kill, no_restart, nullptr);
          }
          p->set_value(std::move(status));
        };
        if (actor_creator_->IsActorInRegistering(actor_id)) {
          actor_creator_->AsyncWaitForActorRegisterFinish(actor_id, std::move(cb));
        } else if (actor_manager_->CheckActorHandleExists(actor_id)) {
          cb(Status::OK());
        } else {
          std::stringstream stream;
          stream << "Failed to find a corresponding actor handle for " << actor_id;
          cb(Status::NotFound(stream.str()));
        }
      },
      "CoreWorker.KillActor");
  const auto &status = f.get();
  // Only call OnActorKilled if the kill was successful (status is OK).
  // If the actor handle doesn't exist, OnActorKilled would crash.
  if (status.ok()) {
    actor_manager_->OnActorKilled(actor_id);
  }
  return status;
}

void CoreWorker::RemoveActorHandleReference(const ActorID &actor_id) {
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  reference_counter_->RemoveLocalReference(actor_handle_id, nullptr);
}

std::optional<rpc::ActorTableData::ActorState> CoreWorker::GetLocalActorState(
    const ActorID &actor_id) const {
  return actor_task_submitter_->GetLocalActorState(actor_id);
}

ActorID CoreWorker::DeserializeAndRegisterActorHandle(const std::string &serialized,
                                                      const ObjectID &outer_object_id,
                                                      bool add_local_ref) {
  auto actor_handle = std::make_unique<ActorHandle>(serialized);
  return actor_manager_->RegisterActorHandle(std::move(actor_handle),
                                             outer_object_id,
                                             CurrentCallSite(),
                                             rpc_address_,
                                             add_local_ref);
}

Status CoreWorker::SerializeActorHandle(const ActorID &actor_id,
                                        std::string *output,
                                        ObjectID *actor_handle_id) const {
  auto actor_handle = actor_manager_->GetActorHandle(actor_id);
  actor_handle->Serialize(output);
  *actor_handle_id = ObjectID::ForActorHandle(actor_id);
  return Status::OK();
}

std::shared_ptr<const ActorHandle> CoreWorker::GetActorHandle(
    const ActorID &actor_id) const {
  return actor_manager_->GetActorHandle(actor_id);
}

std::pair<std::shared_ptr<const ActorHandle>, Status> CoreWorker::GetNamedActorHandle(
    const std::string &name, const std::string &ray_namespace) {
  RAY_CHECK(!name.empty());
  return actor_manager_->GetNamedActorHandle(
      name,
      ray_namespace.empty() ? worker_context_->GetCurrentJobConfig().ray_namespace()
                            : ray_namespace,
      CurrentCallSite(),
      rpc_address_);
}

std::pair<std::vector<std::pair<std::string, std::string>>, Status>
CoreWorker::ListNamedActors(bool all_namespaces) {
  std::vector<std::pair<std::string, std::string>> actors;

  // This call needs to be blocking because we can't return until we get the
  // response from the RPC.
  const auto ray_namespace = worker_context_->GetCurrentJobConfig().ray_namespace();
  auto status =
      gcs_client_->Actors().SyncListNamedActors(all_namespaces, ray_namespace, actors);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in getting the list of named actors, "
              "probably because the GCS server is dead or under high load .";
    return std::make_pair(std::move(actors), Status::TimedOut(stream.str()));
  }
  return std::make_pair(std::move(actors), std::move(status));
}

std::string CoreWorker::GetActorName() const {
  absl::MutexLock lock(&mutex_);
  return actor_manager_->GetActorHandle(actor_id_)->GetName();
}

ResourceMappingType CoreWorker::GetResourceIDs() const {
  absl::MutexLock lock(&mutex_);
  return resource_ids_;
}

std::unique_ptr<worker::ProfileEvent> CoreWorker::CreateProfileEvent(
    const std::string &event_name) {
  return std::make_unique<worker::ProfileEvent>(*task_event_buffer_,
                                                *worker_context_,
                                                options_.node_ip_address,
                                                event_name,
                                                clock_);
}

void CoreWorker::RunTaskExecutionLoop() {
  auto signal_checker = PeriodicalRunner::Create(task_execution_service_);
  if (options_.check_signals) {
    signal_checker->RunFnPeriodically(
        [this] {
          /// The overhead of this is only a single digit microsecond.
          if (worker_context_->GetCurrentActorShouldExit()) {
            Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
                 "User requested to exit the actor.",
                 nullptr);
          }
          auto status = options_.check_signals();
          if (status.IsIntentionalSystemExit()) {
            Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
                 absl::StrCat("Worker exits by a signal. ", status.message()),
                 nullptr);
          }
          if (status.IsUnexpectedSystemExit()) {
            Exit(
                rpc::WorkerExitType::SYSTEM_ERROR,
                absl::StrCat("Worker exits unexpectedly by a signal. ", status.message()),
                nullptr);
          }
        },
        10,
        "CoreWorker.CheckSignal");
  }
  event_loops_running_ = true;
  task_execution_service_.run();
  RAY_CHECK(shutdown_coordinator_ && shutdown_coordinator_->IsShuttingDown())
      << "Task execution loop was terminated without calling shutdown API.";
}

Status CoreWorker::AllocateReturnObject(const ObjectID &object_id,
                                        const size_t &data_size,
                                        const std::shared_ptr<Buffer> &metadata,
                                        const std::vector<ObjectID> &contained_object_ids,
                                        const rpc::Address &owner_address,
                                        int64_t *task_output_inlined_bytes,
                                        std::shared_ptr<RayObject> *return_object) {
  bool object_already_exists = false;
  std::shared_ptr<Buffer> data_buffer;
  if (data_size > 0) {
    RAY_LOG(DEBUG).WithField(object_id) << "Creating return object";
    // Mark this object as containing other object IDs. The ref counter will
    // keep the inner IDs in scope until the outer one is out of scope.
    if (!contained_object_ids.empty()) {
      // Due to response loss caused by network failures,
      // this method may be called multiple times for the same return object
      // but it's fine since AddNestedObjectIds is idempotent.
      // See https://github.com/ray-project/ray/issues/57997
      reference_counter_->AddNestedObjectIds(
          object_id, contained_object_ids, owner_address);
    }

    // Allocate a buffer for the return object.
    if (static_cast<int64_t>(data_size) < max_direct_call_object_size_ &&
        // ensure we don't exceed the limit if we allocate this object inline.
        (*task_output_inlined_bytes + static_cast<int64_t>(data_size) <=
         RayConfig::instance().task_rpc_inlined_bytes_limit())) {
      data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);
      *task_output_inlined_bytes += static_cast<int64_t>(data_size);
    } else {
      RAY_RETURN_NOT_OK(CreateExisting(metadata,
                                       data_size,
                                       object_id,
                                       owner_address,
                                       &data_buffer,
                                       /*created_by_worker=*/true));
      object_already_exists = data_buffer == nullptr;
    }
  }
  // Leave the return object as a nullptr if the object already exists.
  if (!object_already_exists) {
    auto contained_refs = GetObjectRefs(contained_object_ids);
    *return_object =
        std::make_shared<RayObject>(data_buffer, metadata, std::move(contained_refs));
  }

  return Status::OK();
}

Status CoreWorker::ExecuteTask(
    const TaskSpecification &task_spec,
    std::optional<ResourceMappingType> resource_ids,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_return_objects,
    std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
    ReferenceCounterInterface::ReferenceTableProto *borrowed_refs,
    bool *is_retryable_error,
    std::string *actor_repr_name,
    std::string *application_error) {
  RAY_LOG(DEBUG) << "Executing task, task info = " << task_spec.DebugString();

  // If the worker is exited via Exit API, we shouldn't execute tasks anymore.
  if (IsExiting()) {
    absl::MutexLock lock(&mutex_);
    return Status::IntentionalSystemExit(
        absl::StrCat("Worker has already exited. Detail: ", exiting_detail_.value()));
  }

  std::vector<std::shared_ptr<RayObject>> args;
  std::vector<rpc::ObjectReference> arg_refs;
  // This includes all IDs that were passed by reference and any IDs that were
  // inlined in the task spec. These references will be pinned during the task
  // execution and unpinned once the task completes. We will notify the caller
  // about any IDs that we are still borrowing by the time the task completes.
  std::vector<ObjectID> borrowed_ids;

  // Extract task name and retry status for metrics reporting.
  // Use GetName() which returns the custom task name if set via .options(name="..."),
  // otherwise falls back to the function descriptor's call string. This ensures
  // consistency with task events reported to the State API / Dashboard.
  std::string func_name = task_spec.GetName();
  bool is_retry = task_spec.IsRetry();

  // Modify the worker's per-function counters. This should be done before updating any
  // substates (running_in_ray_get, running_in_ray_wait, getting_and_pinning_args) since
  // the metric callback subtracts substate counts from running_total. Incrementing
  // substates before kRunning could result in wrong values of RUNNING metric.
  task_counter_.MovePendingToRunning(func_name, is_retry);

  ++num_get_pin_args_in_flight_;
  task_counter_.SetMetricStatus(
      func_name, rpc::TaskStatus::GETTING_AND_PINNING_ARGS, is_retry);
  Status pin_args_request_status =
      GetAndPinArgsForExecutor(task_spec, &args, &arg_refs, &borrowed_ids);
  task_counter_.UnsetMetricStatus(
      func_name, rpc::TaskStatus::GETTING_AND_PINNING_ARGS, is_retry);
  --num_get_pin_args_in_flight_;
  if (!pin_args_request_status.ok()) {
    ++num_failed_get_pin_args_;
    // If this has happened, it's because we are unable to talk to our local raylet.
    // This very likely means that the raylet has shutdown before this worker
    // unexpectedly. In which case we'll mark the task finished and trigger shut down.
    task_counter_.MoveRunningToFinished(func_name, task_spec.IsRetry());
    Exit(rpc::WorkerExitType::SYSTEM_ERROR,
         absl::StrCat("Worker failed to get and pin task arguments! Error message: ",
                      pin_args_request_status.message()),
         nullptr);
    return pin_args_request_status;
  }

  task_queue_length_ -= 1;
  num_executed_tasks_ += 1;

  worker::TaskStatusEvent::TaskStateUpdate update;
  {
    absl::MutexLock lock(&mutex_);
    update = (task_spec.IsActorTask() && !actor_repr_name_.empty())
                 ? worker::TaskStatusEvent::TaskStateUpdate(actor_repr_name_, pid_)
                 : worker::TaskStatusEvent::TaskStateUpdate(pid_);
  }

  RAY_UNUSED(
      task_event_buffer_->RecordTaskStatusEventIfNeeded(task_spec.TaskId(),
                                                        task_spec.JobId(),
                                                        task_spec.AttemptNumber(),
                                                        task_spec,
                                                        rpc::TaskStatus::RUNNING,
                                                        /*include_task_info=*/false,
                                                        update));

  worker_context_->SetCurrentTask(task_spec);
  SetCurrentTaskId(task_spec.TaskId(), task_spec.AttemptNumber(), task_spec.GetName());

  {
    absl::MutexLock lock(&mutex_);
    running_tasks_.emplace(task_spec.TaskId(), task_spec);
    if (resource_ids.has_value()) {
      resource_ids_ = std::move(*resource_ids);
    }
  }

  RayFunction func{task_spec.GetLanguage(), task_spec.FunctionDescriptor()};

  for (size_t i = 0; i < task_spec.NumReturns(); i++) {
    return_objects->emplace_back(task_spec.ReturnId(i), nullptr);
  }
  // For dynamic tasks, pass the return IDs that were dynamically generated on
  // the first execution.
  if (!task_spec.ReturnsDynamic()) {
    dynamic_return_objects = nullptr;
  } else if (task_spec.AttemptNumber() > 0) {
    for (const auto &dynamic_return_id : task_spec.DynamicReturnIds()) {
      // Increase the put index so that when the generator creates a new obj
      // the object id won't conflict.
      worker_context_->GetNextPutIndex();
      dynamic_return_objects->emplace_back(dynamic_return_id,
                                           std::shared_ptr<RayObject>());
      RAY_LOG(DEBUG) << "Re-executed task " << task_spec.TaskId()
                     << " should return dynamic object " << dynamic_return_id;

      AddLocalReference(dynamic_return_id, "<temporary (DynamicObjectRefGenerator)>");
      reference_counter_->AddBorrowedObject(
          dynamic_return_id, ObjectID::Nil(), task_spec.CallerAddress());
    }
  }

  TaskType task_type = TaskType::NORMAL_TASK;
  if (task_spec.IsActorCreationTask()) {
    task_type = TaskType::ACTOR_CREATION_TASK;
    SetActorId(task_spec.ActorCreationId());
    task_counter_.BecomeActor(task_spec.FunctionDescriptor()->ClassName());
    {
      auto self_actor_handle =
          std::make_unique<ActorHandle>(task_spec.GetSerializedActorHandle());
      // Register the handle to the current actor itself.
      actor_manager_->RegisterActorHandle(std::move(self_actor_handle),
                                          ObjectID::Nil(),
                                          CurrentCallSite(),
                                          rpc_address_,
                                          /*add_local_ref=*/false,
                                          /*is_self=*/true);
    }
    int64_t actor_generator_bp = task_spec.ActorGeneratorBackpressureNumObjects();
    if (actor_generator_bp > 0) {
      // Shared waiter for all streaming-generator tasks on this actor.
      // check_signals matches what the per-task waiter uses (set in
      // CoreWorkerOptions from _raylet.pyx), so KeyboardInterrupt /
      // SystemExit propagate through ReserveSlot's wait loop.
      actor_generator_waiter_ = std::make_shared<ActorWideGeneratorBackpressureWaiter>(
          actor_generator_bp, options_.check_signals);
    }
    RAY_LOG(INFO).WithField(task_spec.ActorCreationId()) << "Creating actor";
  } else if (task_spec.IsActorTask()) {
    task_type = TaskType::ACTOR_TASK;
  }

  std::shared_ptr<LocalMemoryBuffer> creation_task_exception_pb_bytes = nullptr;

  std::vector<ConcurrencyGroup> defined_concurrency_groups = {};
  std::string name_of_concurrency_group_to_execute;
  if (task_spec.IsActorCreationTask()) {
    defined_concurrency_groups = task_spec.ConcurrencyGroups();
  } else if (task_spec.IsActorTask()) {
    name_of_concurrency_group_to_execute = task_spec.ConcurrencyGroupName();
  }

  Status status = options_.task_execution_callback(
      task_spec.CallerAddress(),
      task_type,
      task_spec.GetName(),
      func,
      task_spec.GetRequiredResources().GetResourceUnorderedMap(),
      args,
      arg_refs,
      task_spec.GetDebuggerBreakpoint(),
      task_spec.GetSerializedRetryExceptionAllowlist(),
      return_objects,
      dynamic_return_objects,
      streaming_generator_returns,
      creation_task_exception_pb_bytes,
      is_retryable_error,
      actor_repr_name,
      application_error,
      defined_concurrency_groups,
      name_of_concurrency_group_to_execute,
      /*is_reattempt=*/task_spec.AttemptNumber() > 0,
      /*is_streaming_generator=*/task_spec.IsStreamingGenerator(),
      /*retry_exception=*/task_spec.ShouldRetryExceptions(),
      /*generator_backpressure_num_objects=*/
      task_spec.GeneratorBackpressureNumObjects(),
      /*num_objects_per_yield=*/task_spec.NumObjectsPerYield(),
      /*tensor_transport=*/task_spec.TensorTransport());

  // Get the reference counts for any IDs that we borrowed during this task,
  // remove the local reference for these IDs, and return the ref count info to
  // the caller. This will notify the caller of any IDs that we (or a nested
  // task) are still borrowing. It will also notify the caller of any new IDs
  // that were contained in a borrowed ID that we (or a nested task) are now
  // borrowing.
  std::vector<ObjectID> deleted;
  if (!borrowed_ids.empty()) {
    reference_counter_->PopAndClearLocalBorrowers(borrowed_ids, borrowed_refs, &deleted);
  }
  if (dynamic_return_objects != nullptr) {
    for (const auto &dynamic_return : *dynamic_return_objects) {
      reference_counter_->PopAndClearLocalBorrowers(
          {dynamic_return.first}, borrowed_refs, &deleted);
    }
  }
  memory_store_->Delete(deleted);

  if (task_spec.IsNormalTask() && reference_counter_->NumObjectIDsInScope() != 0) {
    RAY_LOG(DEBUG).WithField(task_spec.TaskId())
        << "There were " << reference_counter_->NumObjectIDsInScope()
        << " ObjectIDs left in scope after executing task. "
           "This is either caused by keeping references to ObjectIDs in Python "
           "between "
           "tasks (e.g., in global variables) or indicates a problem with Ray's "
           "reference counting, and may cause problems in the object store.";
  }

  SetCurrentTaskId(TaskID::Nil(), /*attempt_number=*/0, /*task_name=*/"");
  worker_context_->ResetCurrentTask();

  {
    absl::MutexLock lock(&mutex_);
    size_t erased = running_tasks_.erase(task_spec.TaskId());
    RAY_CHECK(erased == 1);
    // Clean up cancellation state for this task
    canceled_tasks_.erase(task_spec.TaskId());
    if (task_spec.IsNormalTask()) {
      resource_ids_.clear();
    }

    // Cache the returned actor repr name as an instance variable.
    // This is currently only used for exporting task events from the actor.
    if (!actor_repr_name->empty()) {
      actor_repr_name_ = *actor_repr_name;
    }
  }

  task_counter_.MoveRunningToFinished(func_name, task_spec.IsRetry());
  RAY_LOG(DEBUG).WithField(task_spec.TaskId())
      << "Finished executing task, status=" << status;

  if (task_spec.IsActorCreationTask()) {
    RAY_CHECK_OK(raylet_ipc_client_->ActorCreationTaskDone())
        << "Unexpected error in IPC to the Raylet; the Raylet has most likely crashed.";
  }

  std::ostringstream stream;
  if (status.IsCreationTaskError()) {
    Exit(rpc::WorkerExitType::USER_ERROR,
         absl::StrCat(
             "Worker exits because there was an exception in the initialization method "
             "(e.g., __init__). Fix the exceptions from the initialization to resolve "
             "the issue. ",
             status.message()),
         creation_task_exception_pb_bytes);
  } else if (status.IsIntentionalSystemExit()) {
    Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
         absl::StrCat("Worker exits by an user request. ", status.message()),
         creation_task_exception_pb_bytes);
  } else if (status.IsUnexpectedSystemExit()) {
    Exit(rpc::WorkerExitType::SYSTEM_ERROR,
         absl::StrCat("Worker exits unexpectedly. ", status.message()),
         creation_task_exception_pb_bytes);
  } else {
    RAY_CHECK_OK(status) << "Unexpected task status type : " << status;
  }
  return status;
}

Status CoreWorker::SealReturnObject(const ObjectID &return_id,
                                    const std::shared_ptr<RayObject> &return_object,
                                    const ObjectID &generator_id,
                                    const rpc::Address &owner_address) {
  RAY_LOG(DEBUG).WithField(return_id) << "Sealing return object";

  RAY_CHECK(return_object);

  Status status = Status::OK();
  auto owner_address_ptr = std::make_unique<rpc::Address>(owner_address);

  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    status = SealExisting(return_id, true, generator_id, owner_address_ptr);
    if (!status.ok()) {
      RAY_LOG(FATAL).WithField(return_id)
          << "Failed to seal object in store: " << status.message();
    }
  }
  return status;
}

void CoreWorker::AsyncDelObjectRefStream(const ObjectID &generator_id) {
  RAY_LOG(DEBUG).WithField(generator_id) << "AsyncDelObjectRefStream";
  if (task_manager_->TryDelObjectRefStream(generator_id)) {
    return;
  }

  {
    // TryDelObjectRefStream is thread safe so no need to hold the lock above.
    absl::MutexLock lock(&generator_ids_pending_deletion_mutex_);
    generator_ids_pending_deletion_.insert(generator_id);
  }
}

void CoreWorker::TryDelPendingObjectRefStreams() {
  absl::MutexLock lock(&generator_ids_pending_deletion_mutex_);

  std::vector<ObjectID> deleted;
  for (const auto &generator_id : generator_ids_pending_deletion_) {
    RAY_LOG(DEBUG).WithField(generator_id)
        << "TryDelObjectRefStream from generator_ids_pending_deletion_";
    if (task_manager_->TryDelObjectRefStream(generator_id)) {
      deleted.push_back(generator_id);
    }
  }

  for (const auto &generator_id : deleted) {
    generator_ids_pending_deletion_.erase(generator_id);
  }
}

Status CoreWorker::TryReadObjectRefStream(const ObjectID &generator_id,
                                          rpc::ObjectReference *object_ref_out) {
  ObjectID object_id;
  const auto &status = task_manager_->TryReadObjectRefStream(generator_id, &object_id);
  RAY_CHECK(object_ref_out != nullptr);
  object_ref_out->set_object_id(object_id.Binary());
  object_ref_out->mutable_owner_address()->CopyFrom(rpc_address_);
  return status;
}

Status CoreWorker::TryReadObjectRefStreamN(const ObjectID &generator_id,
                                           int64_t num_items) {
  return task_manager_->TryReadObjectRefStreamN(generator_id, num_items);
}

bool CoreWorker::StreamingGeneratorIsFinished(const ObjectID &generator_id) const {
  return task_manager_->StreamingGeneratorIsFinished(generator_id);
}

std::pair<rpc::ObjectReference, bool> CoreWorker::PeekObjectRefStream(
    const ObjectID &generator_id) {
  auto [object_id, ready] = task_manager_->PeekObjectRefStream(generator_id);
  rpc::ObjectReference object_ref;
  object_ref.set_object_id(object_id.Binary());
  object_ref.mutable_owner_address()->CopyFrom(rpc_address_);
  return {object_ref, ready};
}

std::vector<std::pair<rpc::ObjectReference, bool>> CoreWorker::PeekObjectRefStreamN(
    const ObjectID &generator_id, int64_t num_items) {
  auto object_ids_and_ready =
      task_manager_->PeekObjectRefStreamN(generator_id, num_items);
  std::vector<std::pair<rpc::ObjectReference, bool>> results;
  results.reserve(object_ids_and_ready.size());
  for (const auto &[object_id, ready] : object_ids_and_ready) {
    rpc::ObjectReference object_ref;
    object_ref.set_object_id(object_id.Binary());
    object_ref.mutable_owner_address()->CopyFrom(rpc_address_);
    results.emplace_back(std::move(object_ref), ready);
  }
  return results;
}

ObjectID CoreWorker::PeekObjectIdStream(const ObjectID &generator_id) {
  return task_manager_->PeekObjectRefStream(generator_id).first;
}

bool CoreWorker::PinExistingReturnObject(const ObjectID &return_id,
                                         std::shared_ptr<RayObject> *return_object,
                                         const ObjectID &generator_id,
                                         const rpc::Address &owner_address) {
  // TODO(swang): If there is already an existing copy of this object, then it
  // might not have the same value as the new copy. It would be better to evict
  // the existing copy here.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;

  // Temporarily set the return object's owner's address. This is needed to retrieve the
  // value from plasma.
  reference_counter_->AddLocalReference(return_id, "<temporary (pin return object)>");
  reference_counter_->AddBorrowedObject(return_id, ObjectID::Nil(), owner_address);

  // Resolve owner address of return id
  std::vector<ObjectID> object_ids = {return_id};
  auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

  Status status =
      plasma_store_provider_->Get(object_ids, owner_addresses, 0, &result_map);
  // Remove the temporary ref.
  RemoveLocalReference(return_id);

  if (result_map.contains(return_id)) {
    *return_object = std::move(result_map[return_id]);
    RAY_LOG(DEBUG) << "Pinning existing return object " << return_id
                   << " owned by worker "
                   << WorkerID::FromBinary(owner_address.worker_id());
    // Keep the object in scope until it's been pinned.
    std::shared_ptr<RayObject> pinned_return_object = *return_object;
    // Asynchronously ask the raylet to pin the object. Note that this can fail
    // if the raylet fails. We expect the owner of the object to handle that
    // case (e.g., by detecting the raylet failure and storing an error).
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address,
        {return_id},
        generator_id,
        [return_id, pinned_return_object](const Status &pin_object_status,
                                          const rpc::PinObjectIDsReply &reply) {
          // RPC to the local raylet should never fail.
          if (!pin_object_status.ok()) {
            RAY_LOG(ERROR) << "Request to local raylet to pin object failed: "
                           << pin_object_status.ToString();
            return;
          }
          if (!reply.successes(0)) {
            RAY_LOG(INFO).WithField(return_id)
                << "Failed to pin existing copy of the task return object. "
                   "This object may get evicted while there are still "
                   "references to it.";
          }
        });
    return true;
  }

  // Failed to get the existing copy of the return object. It must have been
  // evicted before we could pin it.
  // TODO(swang): We should allow the owner to retry this task instead of
  // immediately returning an error to the application.
  return false;
}

ObjectID CoreWorker::AllocateDynamicReturnId(const rpc::Address &owner_address,
                                             const TaskID &task_id,
                                             std::optional<ObjectIDIndexType> put_index) {
  const auto return_id = worker_context_->GetGeneratorReturnId(task_id, put_index);
  AddLocalReference(return_id, "<temporary (DynamicObjectRefGenerator)>");
  reference_counter_->AddBorrowedObject(return_id, ObjectID::Nil(), owner_address);
  return return_id;
}

Status CoreWorker::ReportGeneratorItemReturns(
    const std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>>
        &dynamic_return_objects,
    const ObjectID &generator_id,
    const rpc::Address &owner_address,
    int64_t item_index,
    uint64_t attempt_number,
    const std::shared_ptr<TaskGeneratorBackpressureWaiter> &waiter,
    const std::shared_ptr<ActorTaskBackpressureMetadata> &actor_metadata) {
  rpc::ReportGeneratorItemReturnsRequest request;
  request.mutable_worker_addr()->CopyFrom(rpc_address_);
  request.set_item_index(item_index);
  request.set_generator_id(generator_id.Binary());
  request.set_attempt_number(attempt_number);
  auto client = core_worker_client_pool_->GetOrConnect(owner_address);

  std::vector<ObjectID> return_ids;
  return_ids.reserve(dynamic_return_objects.size());
  for (const auto &dynamic_return_object : dynamic_return_objects) {
    if (dynamic_return_object.first.IsNil()) {
      continue;
    }
    SerializeReturnObject(dynamic_return_object.first,
                          dynamic_return_object.second,
                          request.add_returned_objects());
    return_ids.push_back(dynamic_return_object.first);
  }
  if (!return_ids.empty()) {
    // When we allocate a dynamic return ID (AllocateDynamicReturnId),
    // we borrow the object. When the object value is allocated, the
    // memory store is updated. We should clear borrowers and memory store
    // here.
    std::vector<ObjectID> deleted;
    ReferenceCounterInterface::ReferenceTableProto borrowed_refs;
    reference_counter_->PopAndClearLocalBorrowers(return_ids, &borrowed_refs, &deleted);
    memory_store_->Delete(deleted);
  }

  const auto return_id = return_ids.empty() ? ObjectID::Nil() : return_ids.front();
  RAY_LOG(DEBUG) << "Write the object ref stream, index: " << item_index
                 << ", id: " << return_id << ", count: " << return_ids.size();

  waiter->IncrementObjectGenerated(return_ids.size());
  const bool needs_consumed_updates =
      waiter->NeedsObjectConsumedUpdates() || actor_metadata != nullptr;
  if (needs_consumed_updates) {
    absl::MutexLock lock(&mutex_);
    auto &state = generator_backpressure_states_[generator_id];
    state.waiter = waiter;
    state.actor_metadata = actor_metadata;
    state.owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
  }

  client->ReportGeneratorItemReturns(
      std::move(request),
      [this, waiter, actor_metadata, generator_id, return_id, item_index](
          const Status &status, const rpc::ReportGeneratorItemReturnsReply &) {
        RAY_LOG(DEBUG) << "ReportGeneratorItemReturns replied. " << generator_id
                       << "index: " << item_index;
        RAY_LOG(DEBUG) << "Total object consumed: " << waiter->TotalObjectConsumed()
                       << ". Total object generated: " << waiter->TotalObjectGenerated();
        if (!status.ok()) {
          // If the request fails, we should just resume until task finishes without
          // backpressure.
          RAY_LOG(WARNING).WithField(return_id)
              << "Failed to report streaming generator return "
                 "to the caller. The yield'ed ObjectRef may not be usable. "
              << status;
        }
        waiter->OnObjectReportAccepted();
        if (!status.ok()) {
          waiter->OnObjectConsumed(waiter->TotalObjectGenerated());
          if (actor_metadata) {
            actor_metadata->Teardown();
          }
          {
            absl::MutexLock lock(&mutex_);
            generator_backpressure_states_.erase(generator_id);
          }
          // The report to the owner failed, so we gave up on backpressure for
          // this task (consumed == generated above). Wake it if an async
          // generator is parked on the per-task wait, plus any actor-wide
          // reserver since the Teardown above freed shared budget.
          NotifyAsyncGeneratorBackpressureUnblock(
              generator_id, /*notify_all=*/actor_metadata != nullptr);
        }
      });

  return Status::OK();
}

void CoreWorker::RegisterGeneratorBackpressureState(
    const ObjectID &generator_id,
    std::shared_ptr<TaskGeneratorBackpressureWaiter> waiter,
    std::shared_ptr<ActorTaskBackpressureMetadata> actor_metadata,
    const rpc::Address &owner_address) {
  absl::MutexLock lock(&mutex_);
  // Only insert if not present. The report path also writes this entry (with
  // the same values); leaving an existing entry untouched avoids racing with
  // any in-progress mutation there.
  auto [it, inserted] = generator_backpressure_states_.try_emplace(generator_id);
  if (!inserted) {
    return;
  }
  it->second.waiter = std::move(waiter);
  it->second.actor_metadata = std::move(actor_metadata);
  it->second.owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
}

void CoreWorker::MarkGeneratorBackpressureTaskFinished(const ObjectID &generator_id) {
  std::shared_ptr<TaskGeneratorBackpressureWaiter> waiter;
  bool keep_until_consumed = false;
  {
    absl::MutexLock lock(&mutex_);
    auto it = generator_backpressure_states_.find(generator_id);
    if (it == generator_backpressure_states_.end()) {
      return;
    }
    it->second.task_finished = true;
    keep_until_consumed = it->second.actor_metadata != nullptr;
    waiter = it->second.waiter;
    if (!keep_until_consumed) {
      generator_backpressure_states_.erase(it);
      return;
    }
  }

  if (waiter->TotalObjectConsumed() >= waiter->TotalObjectGenerated()) {
    absl::MutexLock lock(&mutex_);
    auto it = generator_backpressure_states_.find(generator_id);
    if (it != generator_backpressure_states_.end() && it->second.task_finished &&
        it->second.waiter->TotalObjectConsumed() >=
            it->second.waiter->TotalObjectGenerated()) {
      generator_backpressure_states_.erase(it);
    }
  }
}

bool CoreWorker::TeardownGeneratorBackpressureTask(const ObjectID &generator_id) {
  std::shared_ptr<ActorTaskBackpressureMetadata> actor_metadata;
  {
    absl::MutexLock lock(&mutex_);
    auto it = generator_backpressure_states_.find(generator_id);
    if (it == generator_backpressure_states_.end()) {
      return false;
    }
    actor_metadata = it->second.actor_metadata;
    generator_backpressure_states_.erase(it);
  }
  if (actor_metadata) {
    actor_metadata->Teardown();
  }
  return true;
}

void CoreWorker::SetAsyncGeneratorBackpressureUnblockNotify(const ObjectID &generator_id,
                                                            void (*fn)(void *),
                                                            void *ctx) {
  absl::MutexLock lock(&generator_backpressure_notification_guard_);
  generator_unblock_notifies_[generator_id] = std::make_pair(fn, ctx);
}

void CoreWorker::ClearAsyncGeneratorBackpressureUnblockNotify(
    const ObjectID &generator_id) {
  absl::MutexLock lock(&generator_backpressure_notification_guard_);
  generator_unblock_notifies_.erase(generator_id);
}

void CoreWorker::NotifyAsyncGeneratorBackpressureUnblock(const ObjectID &generator_id,
                                                         bool notify_all) {
  // Hold generator_backpressure_notification_guard_ across the callback(s): this both
  // excludes a concurrent ClearAsyncGeneratorBackpressureUnblockNotify (so the borrowed
  // ctx stays valid for the duration of the call) and keeps a consistent lock
  // order, since the mutex is always taken with the GIL released and the
  // callback acquires the GIL only after.
  absl::MutexLock lock(&generator_backpressure_notification_guard_);
  if (notify_all) {
    for (const auto &[id, cb] : generator_unblock_notifies_) {
      if (cb.first != nullptr) {
        cb.first(cb.second);
      }
    }
  } else {
    auto it = generator_unblock_notifies_.find(generator_id);
    if (it != generator_unblock_notifies_.end() && it->second.first != nullptr) {
      it->second.first(it->second.second);
    }
  }
}

void CoreWorker::HandleReportGeneratorItemReturns(
    rpc::ReportGeneratorItemReturnsRequest request,
    rpc::ReportGeneratorItemReturnsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto generator_id = ObjectID::FromBinary(request.generator_id());
  const auto worker_id = WorkerID::FromBinary(request.worker_addr().worker_id());
  const auto worker_addr = request.worker_addr();
  const auto reply_generator_id = generator_id;
  const auto consumption_generator_id = generator_id;
  task_manager_->HandleReportGeneratorItemReturns(
      request,
      /*execution_signal_callback=*/
      [worker_id,
       generator_id = reply_generator_id,
       send_reply_callback = std::move(send_reply_callback)](const Status &status) {
        RAY_LOG(DEBUG) << "Reply HandleReportGeneratorItemReturns to signal "
                          "executor to resume tasks. "
                       << generator_id << ". Worker ID: " << worker_id;
        send_reply_callback(status, nullptr, nullptr);
      },
      /*consumption_update_callback=*/
      [this, worker_addr, generator_id = consumption_generator_id](
          const Status &status, int64_t total_num_object_consumed) {
        rpc::UpdateGeneratorBackpressureConsumedRequest update_request;
        update_request.set_generator_id(generator_id.Binary());
        update_request.set_total_num_object_consumed(
            status.ok() ? total_num_object_consumed : -1);
        auto client = core_worker_client_pool_->GetOrConnect(worker_addr);
        client->UpdateGeneratorBackpressureConsumed(
            std::move(update_request),
            [generator_id](const Status &update_status,
                           const rpc::UpdateGeneratorBackpressureConsumedReply &) {
              if (!update_status.ok()) {
                // The retryable RPC layer retries transient failures; a
                // permanent failure usually means the executor is gone, in
                // which case no one is blocked on the corresponding
                // WaitUntilObjectConsumed. Still WARN so unexpected
                // executor-side stalls (if any) are visible.
                RAY_LOG(WARNING).WithField(generator_id)
                    << "Failed to update generator consumed progress: " << update_status;
              }
            });
      });
}

void CoreWorker::HandleUpdateGeneratorBackpressureConsumed(
    rpc::UpdateGeneratorBackpressureConsumedRequest request,
    rpc::UpdateGeneratorBackpressureConsumedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto generator_id = ObjectID::FromBinary(request.generator_id());
  std::shared_ptr<TaskGeneratorBackpressureWaiter> waiter;
  std::shared_ptr<ActorTaskBackpressureMetadata> actor_metadata;
  {
    absl::MutexLock lock(&mutex_);
    auto it = generator_backpressure_states_.find(generator_id);
    if (it != generator_backpressure_states_.end()) {
      waiter = it->second.waiter;
      actor_metadata = it->second.actor_metadata;
    }
  }

  const bool teardown = request.total_num_object_consumed() < 0;
  if (waiter) {
    const auto total_num_object_consumed =
        teardown ? waiter->TotalObjectGenerated() : request.total_num_object_consumed();
    waiter->OnObjectConsumed(total_num_object_consumed);
    if (actor_metadata) {
      if (teardown) {
        actor_metadata->Teardown();
      } else {
        actor_metadata->OnConsumed(total_num_object_consumed);
      }
    }

    // Snapshot the waiter's counters BEFORE re-acquiring mutex_ below.
    // TotalObjectConsumed()/TotalObjectGenerated() each lock the waiter's own
    // internal mutex, so calling them while holding mutex_ acquires locks in
    // the order (mutex_ -> waiter mutex). The generator-execution path takes
    // them in the opposite order (waiter mutex held while a callback re-enters
    // mutex_), so the two orderings form a cycle and can potentially deadlock.
    const bool all_objects_consumed =
        waiter->TotalObjectConsumed() >= waiter->TotalObjectGenerated();

    {
      absl::MutexLock lock(&mutex_);
      auto it = generator_backpressure_states_.find(generator_id);
      if (it != generator_backpressure_states_.end() &&
          (teardown ||
           (it->second.task_finished &&
            (it->second.actor_metadata == nullptr || all_objects_consumed)))) {
        generator_backpressure_states_.erase(it);
      }
    }

    // Wake any async streaming generator parked on an asyncio.Event. Done after
    // releasing mutex_ (the callback acquires the GIL). When this task uses
    // the actor-wide cap, its consumption frees shared budget, so wake every
    // registered async generator to re-check; otherwise only this one.
    NotifyAsyncGeneratorBackpressureUnblock(generator_id,
                                            /*notify_all=*/actor_metadata != nullptr);
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

Status CoreWorker::GetAndPinArgsForExecutor(const TaskSpecification &task,
                                            std::vector<std::shared_ptr<RayObject>> *args,
                                            std::vector<rpc::ObjectReference> *arg_refs,
                                            std::vector<ObjectID> *borrowed_ids) {
  auto num_args = task.NumArgs();
  args->reserve(num_args);
  arg_refs->reserve(num_args);

  absl::flat_hash_set<ObjectID> by_ref_ids;
  absl::flat_hash_map<ObjectID, std::vector<size_t>> by_ref_indices;

  for (size_t i = 0; i < task.NumArgs(); ++i) {
    if (task.ArgByRef(i)) {
      const auto &arg_ref = task.ArgRef(i);
      const auto arg_id = ObjectID::FromBinary(arg_ref.object_id());
      by_ref_ids.insert(arg_id);
      by_ref_indices[arg_id].push_back(i);
      arg_refs->push_back(arg_ref);
      args->emplace_back();
      // Pin all args passed by reference for the duration of the task.  This
      // ensures that when the task completes, we can retrieve metadata about
      // any borrowed ObjectIDs that were serialized in the argument's value.
      RAY_LOG(DEBUG).WithField(arg_id) << "Incrementing ref for argument ID";
      reference_counter_->AddLocalReference(arg_id, task.CallSiteString());
      // Attach the argument's owner's address. This is needed to retrieve the
      // value from plasma.
      reference_counter_->AddBorrowedObject(
          arg_id, ObjectID::Nil(), task.ArgRef(i).owner_address());





      borrowed_ids->push_back(arg_id);
      // We need to put an OBJECT_IN_PLASMA error here so the subsequent call to Get()
      // properly redirects to the plasma store.
      // NOTE: This needs to be done after adding reference to reference counter
      // otherwise, the put is a no-op.
      memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         task.ArgObjectId(i),
                         reference_counter_->HasReference(task.ArgObjectId(i)));
    } else {
      // A pass-by-value argument.
      std::shared_ptr<LocalMemoryBuffer> data = nullptr;
      if (task.ArgDataSize(i) != 0u) {
        data = std::make_shared<LocalMemoryBuffer>(const_cast<uint8_t *>(task.ArgData(i)),
                                                   task.ArgDataSize(i));
      }
      std::shared_ptr<LocalMemoryBuffer> metadata = nullptr;
      if (task.ArgMetadataSize(i) != 0u) {
        metadata = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(task.ArgMetadata(i)), task.ArgMetadataSize(i));
      }
      // NOTE: this is a workaround to avoid an extra copy for Java workers.
      // Python workers need this copy to pass test case
      // test_inline_arg_memory_corruption.
      bool copy_data = options_.language == Language::PYTHON;
      auto tensor_transport = task.ArgTensorTransport(i);
      args->push_back(std::make_shared<RayObject>(std::move(data),
                                                  std::move(metadata),
                                                  task.ArgInlinedRefs(i),
                                                  copy_data,
                                                  std::move(tensor_transport)));
      auto &arg_ref = arg_refs->emplace_back();
      arg_ref.set_object_id(task.ArgObjectIdBinary(i));
      // The task borrows all ObjectIDs that were serialized in the inlined
      // arguments. The task will receive references to these IDs, so it is
      // possible for the task to continue borrowing these arguments by the
      // time it finishes.
      for (const auto &inlined_ref : task.ArgInlinedRefs(i)) {
        const auto inlined_id = ObjectID::FromBinary(inlined_ref.object_id());
        RAY_LOG(DEBUG).WithField(inlined_id) << "Incrementing ref for borrowed ID";
        // We do not need to add the ownership information here because it will
        // get added once the language frontend deserializes the value, before
        // the ObjectID can be used.
        reference_counter_->AddLocalReference(inlined_id, task.CallSiteString());
        borrowed_ids->push_back(inlined_id);
      }
    }
  }

  // Fetch by-reference task arguments.
  //
  // Normal executor argument fetching historically goes directly through
  // Plasma. Recovery-succession replay requires a recovery-aware fetch,
  // because a replayed downstream task may depend on an upstream object
  // whose original owner has died.
  //
  // Recovery replays increment the task attempt number, so IsRetry() lets
  // us keep the normal fast path unchanged for original executions.
  // Fetch by-reference arguments directly from the plasma store.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;

  // Resolve owner addresses of by-ref ids.
  std::vector<ObjectID> object_ids(
      by_ref_ids.begin(),
      by_ref_ids.end());

  auto owner_addresses =
      reference_counter_->GetOwnerAddresses(object_ids);

  RAY_RETURN_NOT_OK(
      plasma_store_provider_->Get(
          object_ids,
          owner_addresses,
          -1,
          &result_map));


  for (const auto &it : result_map) {
    for (size_t idx : by_ref_indices[it.first]) {
      args->at(idx) = it.second;
    }
  }

  return Status::OK();
}

void CoreWorker::HandlePushTask(rpc::PushTaskRequest request,
                                rpc::PushTaskReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG).WithField(TaskID::FromBinary(request.task_spec().task_id()))
      << "Received Handle Push Task";
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      RecoverySuccessionManager::CarriesRecoveryMetadata(
          request.task_spec())) {
    auto candidate_reports =
        recovery_succession_manager_->RegisterExecutorTask(request.task_spec());

    for (auto &candidate_report : candidate_reports) {
      // Patch 4E: queue by coordinator and physically coalesce independent
      // logical reports across tasks. The task itself is not blocked on this RPC.
      QueueRecoveryCandidateReport(
          std::move(candidate_report.coordinator_address),
          std::move(candidate_report.request));
    }
  }

  // Set actor info in the worker context.
  if (request.task_spec().type() == TaskType::ACTOR_CREATION_TASK) {
    auto actor_id =
        ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

    // Handle duplicate actor creation tasks that might be sent from the GCS on restart.
    // Ignore the message and reply OK.
    if (worker_context_->GetCurrentActorID() == actor_id) {
      RAY_LOG(INFO) << "Ignoring duplicate actor creation task for actor " << actor_id
                    << ". This is likely due to a GCS server restart.";
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }
    worker_context_->SetCurrentActorId(actor_id);
  }

  // Set job info in the worker context.
  if (request.task_spec().type() == TaskType::ACTOR_CREATION_TASK ||
      request.task_spec().type() == TaskType::NORMAL_TASK) {
    auto job_id = JobID::FromBinary(request.task_spec().job_id());
    worker_context_->MaybeInitializeJobInfo(job_id, request.task_spec().job_config());
    task_counter_.SetJobId(job_id);
  }

  // Increment the task_queue_length and per function counter.
  // Use task name which includes custom name from .options(name="...") if set,
  // ensuring consistency with task events reported to the State API / Dashboard.
  task_queue_length_ += 1;
  std::string func_name = request.task_spec().name();
  task_counter_.IncPending(func_name, request.task_spec().attempt_number() > 0);

  // For actor tasks, we just need to post a HandleActorTask instance to the task
  // execution service.
  if (request.task_spec().type() == TaskType::ACTOR_TASK) {
    // Fire the args-fetch IPC eagerly on the gRPC handler thread so it is
    // pipelined with any in-progress work on the task execution service.
    task_receiver_->BeginActorTaskArgsFetch(request);
    task_execution_service_.post(
        [this,
         request = std::move(request),
         reply,
         send_reply_callback = std::move(send_reply_callback),
         func_name]() mutable {
          // We have posted an exit task onto the main event loop,
          // so shouldn't bother executing any further work.
          if (IsExiting()) {
            RAY_LOG(INFO) << "Queued task " << func_name
                          << " won't be executed because the worker already exited.";
            return;
          }
          task_receiver_->QueueTaskForExecution(
              std::move(request), reply, send_reply_callback);
        },
        "CoreWorker.HandlePushTaskActor");
  } else {
    // Normal tasks are enqueued here, and we post a ExecuteQueuedNormalTasks instance
    // to the task execution service.
    task_receiver_->QueueTaskForExecution(std::move(request), reply, send_reply_callback);
    task_execution_service_.post(
        [this, func_name] {
          // We have posted an exit task onto the main event loop,
          // so shouldn't bother executing any further work.
          if (IsExiting()) {
            RAY_LOG(INFO) << "Queued task " << func_name
                          << " won't be executed because the worker already exited.";
            return;
          }
          task_receiver_->ExecuteQueuedNormalTasks();
        },
        "CoreWorker.HandlePushTask");
  }
}

void CoreWorker::HandleActorCallArgWaitComplete(
    rpc::ActorCallArgWaitCompleteRequest request,
    rpc::ActorCallArgWaitCompleteReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Post on the task execution event loop since this may trigger the
  // execution of a task that is now ready to run.
  task_execution_service_.post(
      [this,
       task_id = TaskID::FromBinary(request.task_id()),
       attempt_number = request.attempt_number()] {
        RAY_LOG(DEBUG).WithField(task_id)
            << "Actor task args are ready for attempt " << attempt_number;
        actor_task_execution_arg_waiter_->MarkReady(TaskAttempt{task_id, attempt_number});
      },
      "CoreWorker.MarkActorTaskArgsReady");

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleRayletNotifyGCSRestart(
    rpc::RayletNotifyGCSRestartRequest request,
    rpc::RayletNotifyGCSRestartReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  gcs_client_->AsyncResubscribe();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

// HandleGetObjectStatus is expected to be idempotent
void CoreWorker::HandleGetObjectStatus(rpc::GetObjectStatusRequest request,
                                       rpc::GetObjectStatusReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.owner_worker_id()),
                           send_reply_callback)) {
    RAY_LOG(INFO) << "Handling GetObjectStatus for object produced by a previous worker "
                     "with the same address";
    return;
  }

  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG).WithField(object_id) << "Received GetObjectStatus";

  if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {
    rpc::RecoveryObjectMetadata metadata;

    if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {
      reply->mutable_recovery_metadata()->CopyFrom(metadata);
    }
  }

  rpc::Address owner_address;
  auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
  if (!has_owner) {
    // We owned this object, but the object has gone out of scope.
    reply->set_status(rpc::GetObjectStatusReply::OUT_OF_SCOPE);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  RAY_CHECK(owner_address.worker_id() == request.owner_worker_id());
  if (reference_counter_->IsPlasmaObjectFreed(object_id)) {
    reply->set_status(rpc::GetObjectStatusReply::FREED);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  // Send the reply once the value has become available. The value is
  // guaranteed to become available eventually because we own the object and
  // its ref count is > 0.
  memory_store_->GetAsync(object_id,
                          [this, object_id, reply, send_reply_callback](
                              const std::shared_ptr<RayObject> &obj) {
                            PopulateObjectStatus(object_id, obj, reply);
                            send_reply_callback(Status::OK(), nullptr, nullptr);
                          });
}

void CoreWorker::PopulateObjectStatus(const ObjectID &object_id,
                                      const std::shared_ptr<RayObject> &obj,
                                      rpc::GetObjectStatusReply *reply) {
  // If obj is the concrete object value, it is small, so we
  // send the object back to the caller in the GetObjectStatus
  // reply, bypassing a Plasma put and object transfer. If obj
  // is an indicator that the object is in Plasma, we set an
  // in_plasma indicator on the message, and the caller will
  // have to facilitate a Plasma object transfer to get the
  // object value.
  auto *object = reply->mutable_object();
  if (obj->HasData()) {
    const auto &data = obj->GetData();
    object->set_data(data->Data(), data->Size());
  }
  if (obj->HasMetadata()) {
    const auto &metadata = obj->GetMetadata();
    object->set_metadata(metadata->Data(), metadata->Size());
  }

  for (const auto &nested_ref : obj->GetNestedRefs()) {
    rpc::ObjectReference *serialized_ref = object->add_nested_inlined_refs();

    serialized_ref->CopyFrom(nested_ref);

    if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr &&
        !nested_ref.object_id().empty()) {
      rpc::RecoveryObjectMetadata metadata;

      const ObjectID nested_object_id = ObjectID::FromBinary(nested_ref.object_id());

      if (TryPopulateRecoveryMetadataForObject(nested_object_id,
                                                                 &metadata)) {
        serialized_ref->mutable_recovery_metadata()->CopyFrom(metadata);
      }
    }
  }

  reply->set_status(rpc::GetObjectStatusReply::CREATED);
  // Set locality data.
  const auto &locality_data = reference_counter_->GetLocalityData(object_id);
  if (locality_data.has_value()) {
    for (const auto &node_id : locality_data.value().nodes_containing_object) {
      reply->add_node_ids(node_id.Binary());
    }
    reply->set_object_size(locality_data.value().object_size);
  }
}


void CoreWorker::SendRecoveryHolderRollback(
    const std::shared_ptr<PendingRecoveryHolderAdmission> &state,
    const rpc::RecoveryManifest &committed_manifest) {
  if (state == nullptr || committed_manifest.task_id().empty() ||
      state->candidate_address.worker_id().empty()) {
    return;
  }

  rpc::CommitRecoveryManifestRequest request;
  request.mutable_manifest()->CopyFrom(committed_manifest);

  auto client = core_worker_client_pool_->GetOrConnect(state->candidate_address);

  uint64_t rpc_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    recovery_succession_manager_->RecordHolderCommitRpcSent(
        static_cast<uint64_t>(committed_manifest.ByteSizeLong()));
    rpc_start_ns = RecoveryProfileNowNs();
  }

  client->CommitRecoveryManifest(
      std::move(request),
      [manager = recovery_succession_manager_, rpc_start_ns](
          const Status &status, rpc::CommitRecoveryManifestReply &&reply) {
        static_cast<void>(reply);
        if (rpc_start_ns != 0) {
          manager->RecordHolderCommitRpcLatency(RecoveryProfileNowNs() - rpc_start_ns);
        }
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Patch 4D provisional-holder rollback RPC failed: "
                         << status;
        }
      });
}

void CoreWorker::AbortRecoveryHolderAdmissionSuffix(
    const std::shared_ptr<PendingRecoveryHolderAdmission> &failed_state,
    rpc::ReportRecoveryCandidateReply::Result failed_result,
    const rpc::RecoveryManifest &committed_manifest) {
  if (failed_state == nullptr) {
    return;
  }


  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !recovery_witness_holder_baseline_enabled_) {
    // Patch 4M-CERT independent failure cleanup: only this certificate fails.
    recovery_succession_manager_->AbortHolderAdmission(
        failed_state->reservation_id);

    {
      absl::MutexLock lock(&recovery_holder_admission_mutex_);
      auto task_it = recovery_holder_admission_states_.find(failed_state->task_id);
      if (task_it != recovery_holder_admission_states_.end()) {
        auto rank_it = task_it->second.pending_by_rank.find(failed_state->rank);
        if (rank_it != task_it->second.pending_by_rank.end() &&
            rank_it->second->reservation_id == failed_state->reservation_id) {
          rank_it->second->aborted = true;
          rank_it->second->abort_manifest.CopyFrom(committed_manifest);
          task_it->second.pending_by_rank.erase(rank_it);
        }
        if (task_it->second.pending_by_rank.empty()) {
          recovery_holder_admission_states_.erase(task_it);
        }
      }
    }

    if (failed_state->reply != nullptr) {
      failed_state->reply->set_result(failed_result);
      if (!committed_manifest.task_id().empty()) {
        failed_state->reply->mutable_latest_manifest()->CopyFrom(committed_manifest);
      }
    }
    SendRecoveryHolderRollback(failed_state, committed_manifest);
    failed_state->send_reply_callback(Status::OK(), nullptr, nullptr);
    TryAdvanceRecoveryHolderAdmissions(failed_state->task_id);
    return;
  }

  // First remove the owner-side reservations. Manager semantics remove the
  // failed rank and every speculative rank above it.
  recovery_succession_manager_->AbortHolderAdmission(failed_state->reservation_id);

  std::vector<std::shared_ptr<PendingRecoveryHolderAdmission>> aborted;
  {
    absl::MutexLock lock(&recovery_holder_admission_mutex_);
    const auto task_it = recovery_holder_admission_states_.find(failed_state->task_id);
    if (task_it != recovery_holder_admission_states_.end()) {
      auto &task_state = task_it->second;
      for (auto it = task_state.pending_by_rank.lower_bound(failed_state->rank);
           it != task_state.pending_by_rank.end();) {
        it->second->aborted = true;
        it->second->abort_manifest.CopyFrom(committed_manifest);
        aborted.push_back(it->second);
        it = task_state.pending_by_rank.erase(it);
      }
      if (task_state.witness_publish_rank >= failed_state->rank) {
        task_state.witness_publish_rank = 0;
      }
      if (task_state.pending_by_rank.empty()) {
        recovery_holder_admission_states_.erase(task_it);
      }
    }
  }

  for (const auto &state : aborted) {
    if (state->reply != nullptr) {
      state->reply->set_result(
          state->rank == failed_state->rank
              ? failed_result
              : rpc::ReportRecoveryCandidateReply::NO_SLOT);
      if (!committed_manifest.task_id().empty()) {
        state->reply->mutable_latest_manifest()->CopyFrom(committed_manifest);
      }
    }

    // Failure-only cleanup. A higher-rank InstallRecoveryHolder may already
    // have completed. Roll it back to the last committed prefix so it does not
    // remain a permanently orphaned provisional holder.
    SendRecoveryHolderRollback(state, committed_manifest);

    state->send_reply_callback(Status::OK(), nullptr, nullptr);
  }
}

void CoreWorker::TryAdvanceRecoveryHolderAdmissions(const TaskID &task_id) {
  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !recovery_witness_holder_baseline_enabled_) {
    std::vector<std::shared_ptr<PendingRecoveryHolderAdmission>> ready;
    {
      absl::MutexLock lock(&recovery_holder_admission_mutex_);
      const auto task_it = recovery_holder_admission_states_.find(task_id);
      if (task_it == recovery_holder_admission_states_.end()) {
        return;
      }
      for (auto &[slot, state] : task_it->second.pending_by_rank) {
        static_cast<void>(slot);
        if (state->installed && !state->aborted && !state->witness_publish_started) {
          state->witness_publish_started = true;
          ready.push_back(state);
        }
      }
    }

    // No rank gate: every installed independent certificate can enter witness
    // confirmation concurrently.
    for (auto &state : ready) {
      FinishRecoveryHolderAdmissionCertificate(std::move(state));
    }
    return;
  }

  // Patch 4D/4K fallback: installs may overlap but witness publication and
  // durable commit remain strictly rank ordered.
  std::shared_ptr<PendingRecoveryHolderAdmission> next;
  {
    absl::MutexLock lock(&recovery_holder_admission_mutex_);
    const auto task_it = recovery_holder_admission_states_.find(task_id);
    if (task_it == recovery_holder_admission_states_.end()) {
      return;
    }

    auto &task_state = task_it->second;
    if (task_state.witness_publish_rank != 0 || task_state.pending_by_rank.empty()) {
      return;
    }

    const auto first = task_state.pending_by_rank.begin();
    if (!first->second->installed || first->second->aborted) {
      return;
    }

    task_state.witness_publish_rank = first->first;
    next = first->second;
  }

  FinishRecoveryHolderAdmission(std::move(next));
}


void CoreWorker::FinishRecoveryHolderAdmissionCertificate(
    std::shared_ptr<PendingRecoveryHolderAdmission> state) {
  if (state == nullptr) {
    return;
  }

  const rpc::RecoveryHolder *holder = nullptr;
  for (const rpc::RecoveryHolder &candidate : state->proposed_manifest.succession()) {
    if (candidate.rank() == state->rank &&
        candidate.address().worker_id() == state->candidate_address.worker_id()) {
      holder = &candidate;
      break;
    }
  }
  if (holder == nullptr) {
    AbortRecoveryHolderAdmissionSuffix(
        state, rpc::ReportRecoveryCandidateReply::STALE_MANIFEST, state->latest_manifest);
    return;
  }

  rpc::RecoveryHolderCertificate certificate;
  certificate.set_task_id(state->proposed_manifest.task_id());
  certificate.set_generation(state->proposed_manifest.version().generation());
  certificate.set_slot(state->rank);
  certificate.mutable_holder()->CopyFrom(*holder);

  auto manager = recovery_succession_manager_;
  const uint64_t publish_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

  PublishRecoveryHolderCertificateToWitnesses(
      state->latest_manifest,
      certificate,
      [this, manager, state, publish_start_ns](
          bool witness_stored,
          std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (publish_start_ns != 0) {
          manager->RecordWitnessPublishLatency(
              RecoveryProfileNowNs() - publish_start_ns);
        }

        if (!witness_stored) {
          rpc::RecoveryManifest rollback;
          if (newer_manifest.has_value()) {
            rollback.CopyFrom(newer_manifest.value());
            manager->ApplyCommittedManifest(newer_manifest.value());
          } else {
            rollback.CopyFrom(state->latest_manifest);
          }
          AbortRecoveryHolderAdmissionSuffix(
              state,
              rpc::ReportRecoveryCandidateReply::STALE_MANIFEST,
              rollback);
          return;
        }

        if (RayConfig::instance().recovery_succession_test_fail_after_witness_ack()) {
          RAY_LOG(WARNING).WithField(state->task_id)
              << "TEST ONLY: Patch 4M-CERT failure after certificate witness ACK";
          state->send_reply_callback(
              Status::IOError(
                  "Injected Patch 4M-CERT failure after witness ACK before owner commit"),
              nullptr,
              nullptr);
          return;
        }

        rpc::RecoveryManifest committed_manifest;
        const uint64_t holder_commit_cpu_start_ns =
            recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
        const bool holder_committed =
            manager->CommitHolderAdmission(state->reservation_id,
                                           &committed_manifest);
        if (holder_commit_cpu_start_ns != 0) {
          manager->RecordHolderCommitCpu(
              RecoveryProfileNowNs() - holder_commit_cpu_start_ns);
        }
        if (!holder_committed) {
          AbortRecoveryHolderAdmissionSuffix(
              state,
              rpc::ReportRecoveryCandidateReply::STALE_MANIFEST,
              state->latest_manifest);
          return;
        }

        if (state->admission_start_ns != 0) {
          manager->RecordHolderAdmissionLatency(
              RecoveryProfileNowNs() - state->admission_start_ns);
        }

        state->reply->set_result(rpc::ReportRecoveryCandidateReply::ACCEPTED);
        state->reply->mutable_latest_manifest()->CopyFrom(committed_manifest);
        state->send_reply_callback(Status::OK(), nullptr, nullptr);

        {
          absl::MutexLock lock(&recovery_holder_admission_mutex_);
          auto task_it = recovery_holder_admission_states_.find(state->task_id);
          if (task_it != recovery_holder_admission_states_.end()) {
            auto rank_it = task_it->second.pending_by_rank.find(state->rank);
            if (rank_it != task_it->second.pending_by_rank.end() &&
                rank_it->second->reservation_id == state->reservation_id) {
              task_it->second.pending_by_rank.erase(rank_it);
            }
            if (task_it->second.pending_by_rank.empty()) {
              recovery_holder_admission_states_.erase(task_it);
            }
          }
        }

        RAY_LOG(INFO).WithField(state->task_id)
            << "Patch 4M-CERT witness-confirmed independent holder slot "
            << state->rank;
        RAY_LOG(INFO).WithField(state->task_id)
            << "Committed recovery succession manifest after witness publication with "
            << committed_manifest.succession_size() << " total members";
        TryAdvanceRecoveryHolderAdmissions(state->task_id);
      });
}

void CoreWorker::FinishRecoveryHolderAdmission(
    std::shared_ptr<PendingRecoveryHolderAdmission> state) {
  if (state == nullptr) {
    return;
  }

  auto manager = recovery_succession_manager_;
  uint64_t witness_publish_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    witness_publish_start_ns = RecoveryProfileNowNs();

    // Benchmark 70: sample whether H2 is already prepared exactly when H1
    // starts publication. This is observation only: H1 is never delayed.
    if (!recovery_witness_holder_baseline_enabled_ &&
        !manager->RecoveryFrontierEnabled() &&
        !RayConfig::instance().enable_recovery_succession_certificate_admission() &&
        state->rank == 1 && state->proposed_manifest.target_holder_count() == 2) {
      bool h2_reserved = false;
      bool h2_installed = false;
      {
        absl::MutexLock lock(&recovery_holder_admission_mutex_);
        const auto task_it = recovery_holder_admission_states_.find(state->task_id);
        if (task_it != recovery_holder_admission_states_.end()) {
          const auto h2_it = task_it->second.pending_by_rank.find(2);
          if (h2_it != task_it->second.pending_by_rank.end() &&
              !h2_it->second->aborted) {
            h2_reserved = true;
            h2_installed = h2_it->second->installed;
          }
        }
      }
      manager->RecordH2ReadinessAtH1Publish(h2_reserved, h2_installed);
    }
  }

  PublishRecoveryManifestToWitnesses(
      state->proposed_manifest,
      [this, manager, state, witness_publish_start_ns](
          bool witness_stored,
          std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (witness_publish_start_ns != 0) {
          manager->RecordWitnessPublishLatency(
              RecoveryProfileNowNs() - witness_publish_start_ns);

          // Benchmark 70: observe whether H2 became prepared while H1 was
          // waiting on witness durability. This is sampled at the successful
          // H1 publication callback, before local H1 commit/bookkeeping.
          if (witness_stored &&
              !recovery_witness_holder_baseline_enabled_ &&
              !manager->RecoveryFrontierEnabled() &&
              !RayConfig::instance()
                   .enable_recovery_succession_certificate_admission() &&
              state->rank == 1 &&
              state->proposed_manifest.target_holder_count() == 2) {
            bool h2_reserved = false;
            bool h2_installed = false;
            {
              absl::MutexLock lock(&recovery_holder_admission_mutex_);
              const auto task_it =
                  recovery_holder_admission_states_.find(state->task_id);
              if (task_it != recovery_holder_admission_states_.end()) {
                const auto h2_it = task_it->second.pending_by_rank.find(2);
                if (h2_it != task_it->second.pending_by_rank.end() &&
                    !h2_it->second->aborted) {
                  h2_reserved = true;
                  h2_installed = h2_it->second->installed;
                }
              }
            }
            manager->RecordH2ReadinessAtH1Ack(
                h2_reserved, h2_installed);
          }
        }

        if (!witness_stored) {
          rpc::RecoveryManifest rollback_manifest;
          if (newer_manifest.has_value()) {
            rollback_manifest.CopyFrom(newer_manifest.value());
            manager->ApplyCommittedManifest(newer_manifest.value());
          } else {
            rollback_manifest.CopyFrom(state->latest_manifest);
          }

          AbortRecoveryHolderAdmissionSuffix(
              state,
              rpc::ReportRecoveryCandidateReply::STALE_MANIFEST,
              rollback_manifest);
          return;
        }

        // Patch 4F remains provisional even without an install RPC. Keep
        // the failure injection active after witness ACK and before the
        // candidate learns the committed generation through the report reply.
        if (RayConfig::instance().recovery_succession_test_fail_after_witness_ack()) {
          RAY_LOG(WARNING).WithField(state->task_id)
              << "TEST ONLY: injected recovery succession failure after "
                 "witness ACK before candidate commit";
          state->send_reply_callback(
              Status::IOError(
                  "Injected recovery succession failure after witness ACK "
                  "before candidate commit"),
              nullptr,
              nullptr);
          return;
        }

        rpc::RecoveryManifest committed_manifest;
        const uint64_t holder_commit_cpu_start_ns =
            recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
        const bool holder_committed =
            manager->CommitHolderAdmission(state->reservation_id,
                                           &committed_manifest);
        if (holder_commit_cpu_start_ns != 0) {
          manager->RecordHolderCommitCpu(
              RecoveryProfileNowNs() - holder_commit_cpu_start_ns);
        }
        if (!holder_committed) {
          // This should not occur on the normal Patch-4D path because only the
          // lowest installed rank reaches this function. Fail the speculative
          // suffix rather than committing out of order.
          AbortRecoveryHolderAdmissionSuffix(
              state,
              rpc::ReportRecoveryCandidateReply::STALE_MANIFEST,
              state->latest_manifest);
          return;
        }

        if (state->admission_start_ns != 0) {
          manager->RecordHolderAdmissionLatency(
              RecoveryProfileNowNs() - state->admission_start_ns);
        }

        state->reply->set_result(rpc::ReportRecoveryCandidateReply::ACCEPTED);
        state->reply->mutable_latest_manifest()->CopyFrom(committed_manifest);

        RAY_LOG(INFO).WithField(state->task_id)
            << "Patch 4D: committed ordered recovery succession rank "
            << state->rank << " with " << committed_manifest.succession_size()
            << " total members";

        // Patch 4B-2 remains in force: successful normal admission does not
        // send an explicit CommitRecoveryManifest RPC. The report reply carries
        // the committed manifest to this candidate.
        state->send_reply_callback(Status::OK(), nullptr, nullptr);

        {
          absl::MutexLock lock(&recovery_holder_admission_mutex_);
          const auto task_it = recovery_holder_admission_states_.find(state->task_id);
          if (task_it != recovery_holder_admission_states_.end()) {
            auto &task_state = task_it->second;
            const auto rank_it = task_state.pending_by_rank.find(state->rank);
            if (rank_it != task_state.pending_by_rank.end() &&
                rank_it->second->reservation_id == state->reservation_id) {
              task_state.pending_by_rank.erase(rank_it);
            }
            if (task_state.witness_publish_rank == state->rank) {
              task_state.witness_publish_rank = 0;
            }
            if (task_state.pending_by_rank.empty()) {
              recovery_holder_admission_states_.erase(task_it);
            }
          }
        }

        TryAdvanceRecoveryHolderAdmissions(state->task_id);
      });
}


void CoreWorker::QueueRecoveryCandidateReport(
    rpc::Address coordinator_address,
    rpc::ReportRecoveryCandidateRequest request) {
  if (request.task_id().empty()) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());
  const uint64_t patch4g_queue_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
  auto patch4g_manager = recovery_succession_manager_;
  absl::Cleanup patch4g_queue_profile =
      [patch4g_manager, patch4g_queue_start_ns] {
        if (patch4g_manager != nullptr && patch4g_queue_start_ns != 0) {
          patch4g_manager->RecordCandidateQueueLatency(
              RecoveryProfileNowNs() - patch4g_queue_start_ns);
        }
      };

  // Preserve deterministic failure-injection semantics. A batch has one gRPC
  // status for all logical items, so the post-witness/pre-commit test continues
  // to use the original single-item RPC path.
  //
  // Patch 4K: H1 now uses the same physical coalescing path as H2+ in full
  // mode. This does NOT change logical admission: every real borrower still
  // contributes exactly one candidate report, and the owner performs the same
  // reservation -> install -> witness -> ordered commit sequence. It only
  // coalesces independent candidate RPCs, which also lets the existing batch
  // server path coalesce the corresponding holder installs.
  //
  if (RayConfig::instance().recovery_succession_test_fail_after_witness_ack() ||
      coordinator_address.worker_id().empty()) {
    auto manager = recovery_succession_manager_;
    auto client = core_worker_client_pool_->GetOrConnect(coordinator_address);
    const uint64_t patch4g_rpc_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
    if (patch4g_rpc_start_ns != 0) {
      manager->RecordCandidateRpcSent(
          1, static_cast<uint64_t>(request.ByteSizeLong()));
    }
    client->ReportRecoveryCandidate(
        std::move(request),
        [manager, task_id, patch4g_rpc_start_ns](
            const Status &status,
            rpc::ReportRecoveryCandidateReply &&candidate_reply) {
          if (patch4g_rpc_start_ns != 0) {
            manager->RecordCandidateRpcLatency(
                1, RecoveryProfileNowNs() - patch4g_rpc_start_ns);
          }
          if (!status.ok()) {
            RAY_LOG(DEBUG).WithField(task_id)
                << "Recovery candidate report failed: " << status;
            return;
          }

          if (candidate_reply.has_latest_manifest()) {
            manager->ApplyCommittedManifest(candidate_reply.latest_manifest());
          }

          if (candidate_reply.result() == rpc::ReportRecoveryCandidateReply::NO_SLOT ||
              candidate_reply.result() ==
                  rpc::ReportRecoveryCandidateReply::STALE_MANIFEST) {
            manager->AllowCandidateReportRetry(task_id);
          }
        });
    return;
  }

  const std::string coordinator_worker_id = coordinator_address.worker_id();
  bool schedule_flush = false;

  {
    absl::MutexLock lock(&recovery_candidate_batch_mutex_);
    auto [it, inserted] = recovery_candidate_batch_queues_.try_emplace(
        coordinator_worker_id);
    RecoveryCandidateBatchQueue &queue = it->second;

    if (inserted) {
      queue.coordinator_address.CopyFrom(coordinator_address);
      schedule_flush = true;
    }

    PendingRecoveryCandidateReport pending;
    pending.task_id = task_id;
    pending.request.Swap(&request);
    queue.pending.push_back(std::move(pending));
  }

  if (schedule_flush) {
    io_service_.post(
        [this, coordinator_worker_id]() {
          FlushRecoveryCandidateReportBatch(coordinator_worker_id);
        },
        "CoreWorker.FlushRecoveryCandidateReportBatch",
        kRecoveryCandidateBatchDelayUs);
  }
}

void CoreWorker::FlushRecoveryCandidateReportBatch(
    const std::string &coordinator_worker_id) {
  rpc::Address coordinator_address;
  std::vector<PendingRecoveryCandidateReport> items;
  bool schedule_next = false;

  {
    absl::MutexLock lock(&recovery_candidate_batch_mutex_);
    const auto it = recovery_candidate_batch_queues_.find(coordinator_worker_id);
    if (it == recovery_candidate_batch_queues_.end()) {
      return;
    }

    RecoveryCandidateBatchQueue &queue = it->second;
    coordinator_address.CopyFrom(queue.coordinator_address);

    const size_t take =
        std::min(kRecoveryCandidateBatchMaxItems, queue.pending.size());
    items.reserve(take);
    for (size_t i = 0; i < take; ++i) {
      items.push_back(std::move(queue.pending.front()));
      queue.pending.pop_front();
    }

    if (queue.pending.empty()) {
      recovery_candidate_batch_queues_.erase(it);
    } else {
      schedule_next = true;
    }
  }

  if (schedule_next) {
    // The first window already performed the coalescing. Drain a backlog on
    // successive event-loop turns without adding another fixed delay.
    io_service_.post(
        [this, coordinator_worker_id]() {
          FlushRecoveryCandidateReportBatch(coordinator_worker_id);
        },
        "CoreWorker.FlushRecoveryCandidateReportBatch");
  }

  if (items.empty()) {
    return;
  }

  auto manager = recovery_succession_manager_;
  auto client = core_worker_client_pool_->GetOrConnect(coordinator_address);

  if (items.size() == 1) {
    TaskID task_id = items.front().task_id;
    rpc::ReportRecoveryCandidateRequest single_request;
    single_request.Swap(&items.front().request);

    const uint64_t patch4g_rpc_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
    if (patch4g_rpc_start_ns != 0) {
      manager->RecordCandidateRpcSent(
          1, static_cast<uint64_t>(single_request.ByteSizeLong()));
    }
    client->ReportRecoveryCandidate(
        std::move(single_request),
        [manager, task_id, patch4g_rpc_start_ns](
            const Status &status,
            rpc::ReportRecoveryCandidateReply &&candidate_reply) {
          if (patch4g_rpc_start_ns != 0) {
            manager->RecordCandidateRpcLatency(
                1, RecoveryProfileNowNs() - patch4g_rpc_start_ns);
          }
          if (!status.ok()) {
            RAY_LOG(DEBUG).WithField(task_id)
                << "Recovery candidate report failed: " << status;
            return;
          }

          if (candidate_reply.has_latest_manifest()) {
            manager->ApplyCommittedManifest(candidate_reply.latest_manifest());
          }

          if (candidate_reply.result() == rpc::ReportRecoveryCandidateReply::NO_SLOT ||
              candidate_reply.result() ==
                  rpc::ReportRecoveryCandidateReply::STALE_MANIFEST) {
            manager->AllowCandidateReportRetry(task_id);
          }
        });
    return;
  }

  rpc::ReportRecoveryCandidateBatchRequest batch_request;
  batch_request.mutable_requests()->Reserve(static_cast<int>(items.size()));
  std::vector<TaskID> task_ids;
  task_ids.reserve(items.size());

  for (auto &item : items) {
    task_ids.push_back(item.task_id);
    batch_request.add_requests()->Swap(&item.request);
  }

  RAY_LOG(DEBUG) << "Patch 4E sending candidate batch with "
                 << task_ids.size() << " logical reports";

  const uint64_t patch4g_rpc_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
  if (patch4g_rpc_start_ns != 0) {
    manager->RecordCandidateRpcSent(
        static_cast<uint64_t>(task_ids.size()),
        static_cast<uint64_t>(batch_request.ByteSizeLong()));
  }
  client->ReportRecoveryCandidateBatch(
      std::move(batch_request),
      [manager,
       task_ids = std::move(task_ids),
       patch4g_rpc_start_ns](
          const Status &status,
          rpc::ReportRecoveryCandidateBatchReply &&batch_reply) mutable {
        if (patch4g_rpc_start_ns != 0) {
          manager->RecordCandidateRpcLatency(
              static_cast<uint64_t>(task_ids.size()),
              RecoveryProfileNowNs() - patch4g_rpc_start_ns);
        }
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Recovery candidate batch failed: " << status;
          return;
        }

        const int reply_count = batch_reply.replies_size();
        const size_t matched =
            std::min(task_ids.size(), static_cast<size_t>(reply_count));

        for (size_t i = 0; i < matched; ++i) {
          const auto &candidate_reply = batch_reply.replies(static_cast<int>(i));

          if (candidate_reply.has_latest_manifest()) {
            manager->ApplyCommittedManifest(candidate_reply.latest_manifest());
          }

          if (candidate_reply.result() == rpc::ReportRecoveryCandidateReply::NO_SLOT ||
              candidate_reply.result() ==
                  rpc::ReportRecoveryCandidateReply::STALE_MANIFEST) {
            manager->AllowCandidateReportRetry(task_ids[i]);
          }
        }

        if (matched != task_ids.size()) {
          RAY_LOG(WARNING)
              << "Patch 4E candidate batch reply size mismatch: expected "
              << task_ids.size() << ", got " << reply_count;

          // Missing logical replies must not permanently suppress future
          // candidate reports from those tasks.
          for (size_t i = matched; i < task_ids.size(); ++i) {
            manager->AllowCandidateReportRetry(task_ids[i]);
          }
        }
      });
}

std::optional<CoreWorker::PreparedRecoveryHolderInstall>
CoreWorker::PrepareRecoveryCandidateAdmission(
    const rpc::ReportRecoveryCandidateRequest &request,
    rpc::ReportRecoveryCandidateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (reply == nullptr) {
    send_reply_callback(Status::Invalid("null recovery candidate reply"), nullptr, nullptr);
    return std::nullopt;
  }

  if (!recovery_succession_enabled_ ||
      recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    reply->set_result(rpc::ReportRecoveryCandidateReply::DISABLED);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return std::nullopt;
  }

  auto manager = recovery_succession_manager_;

  const uint64_t admission_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

  RecoverySuccessionManager::HolderAdmissionPlan admission_plan;
  rpc::RecoveryManifest latest_manifest;

  std::optional<TaskSpecification> owner_task_spec;
  const rpc::TaskSpec *owner_task_proto = nullptr;
  if (!request.already_stores_task_spec() &&
      request.task_id().size() == TaskID::Size()) {
    owner_task_spec =
        task_manager_->GetTaskSpec(TaskID::FromBinary(request.task_id()));
    if (owner_task_spec.has_value()) {
      owner_task_proto = &owner_task_spec->GetMessage();
    }
  }

  const uint64_t holder_prepare_cpu_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
  const auto result = manager->PrepareHolderAdmission(
      request, owner_task_proto, &admission_plan, &latest_manifest);
  if (holder_prepare_cpu_start_ns != 0) {
    manager->RecordHolderAdmissionPrepareCpu(
        RecoveryProfileNowNs() - holder_prepare_cpu_start_ns);
  }

  if (recovery_succession_profiling_enabled_) {
    const bool accepted_new_holder =
        result == rpc::ReportRecoveryCandidateReply::ACCEPTED &&
        !admission_plan.reservation_id.empty();
    manager->RecordCandidateReport(accepted_new_holder);
  }

  reply->set_result(result);
  if (!latest_manifest.task_id().empty()) {
    reply->mutable_latest_manifest()->CopyFrom(latest_manifest);
  }

  // ACCEPTED with no reservation means already committed or already pending.
  if (result != rpc::ReportRecoveryCandidateReply::ACCEPTED ||
      admission_plan.reservation_id.empty()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return std::nullopt;
  }

  const std::string reservation_id = admission_plan.reservation_id;
  const TaskID task_id = TaskID::FromBinary(admission_plan.proposed_manifest.task_id());

  rpc::Address candidate_address;
  candidate_address.CopyFrom(admission_plan.candidate_address);

  rpc::RecoveryManifest proposed_manifest;
  proposed_manifest.CopyFrom(admission_plan.proposed_manifest);

  const rpc::RecoveryHolder *candidate_holder = nullptr;
  for (const rpc::RecoveryHolder &holder : proposed_manifest.succession()) {
    if (holder.address().worker_id() == candidate_address.worker_id()) {
      candidate_holder = &holder;
      break;
    }
  }

  if (candidate_holder == nullptr) {
    manager->AbortHolderAdmission(reservation_id);
    reply->set_result(rpc::ReportRecoveryCandidateReply::STALE_MANIFEST);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return std::nullopt;
  }

  auto state = std::make_shared<PendingRecoveryHolderAdmission>();
  state->reservation_id = reservation_id;
  state->task_id = task_id;
  state->rank = candidate_holder->rank();
  state->candidate_address.CopyFrom(candidate_address);
  state->latest_manifest.CopyFrom(latest_manifest);
  state->proposed_manifest.CopyFrom(proposed_manifest);
  state->admission_start_ns = admission_start_ns;
  state->reply = reply;
  state->send_reply_callback = std::move(send_reply_callback);

  {
    absl::MutexLock lock(&recovery_holder_admission_mutex_);
    auto &task_state = recovery_holder_admission_states_[task_id];
    const auto inserted = task_state.pending_by_rank.emplace(state->rank, state);
    if (!inserted.second) {
      manager->AbortHolderAdmission(reservation_id);
      state->reply->set_result(rpc::ReportRecoveryCandidateReply::NO_SLOT);
      state->send_reply_callback(Status::OK(), nullptr, nullptr);
      return std::nullopt;
    }
  }

  if (admission_plan.candidate_already_stores_task_spec) {
    {
      absl::MutexLock lock(&recovery_holder_admission_mutex_);
      state->installed = true;
    }
    TryAdvanceRecoveryHolderAdmissions(task_id);
    return std::nullopt;
  }

  rpc::InstallRecoveryHolderRequest install_request;
  install_request.set_task_id(proposed_manifest.task_id());
  install_request.set_reservation_id(reservation_id);
  install_request.set_proposed_rank(state->rank);

  if (recovery_succession_profiling_enabled_) {
    const uint64_t task_spec_copy_start_ns = RecoveryProfileNowNs();
    install_request.mutable_task_spec()->CopyFrom(admission_plan.task_spec);
    manager->RecordOwnerTaskSpecCopyLatency(
        RecoveryProfileNowNs() - task_spec_copy_start_ns);
  } else {
    install_request.mutable_task_spec()->CopyFrom(admission_plan.task_spec);
  }

  install_request.mutable_proposed_manifest()->CopyFrom(proposed_manifest);

  if (recovery_succession_profiling_enabled_) {
    // Keep the existing logical accounting. Patch 4E changes physical RPC count,
    // not the number of logical holder installations or lineage bytes.
    manager->RecordHolderInstallRpcSent(
        static_cast<uint64_t>(install_request.task_spec().ByteSizeLong()),
        static_cast<uint64_t>(install_request.proposed_manifest().ByteSizeLong()));
  }

  PreparedRecoveryHolderInstall prepared;
  prepared.state = std::move(state);
  prepared.request.Swap(&install_request);
  return prepared;
}

void CoreWorker::HandleRecoveryHolderInstallResult(
    const std::shared_ptr<PendingRecoveryHolderAdmission> &state,
    const Status &status,
    rpc::InstallRecoveryHolderReply install_reply) {
  if (state == nullptr) {
    return;
  }

  auto manager = recovery_succession_manager_;
  if (state->install_start_ns != 0) {
    manager->RecordHolderInstallRpcLatency(
        RecoveryProfileNowNs() - state->install_start_ns);
    state->install_start_ns = 0;
  }
  const uint64_t callback_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
  absl::Cleanup callback_profile = [&manager, callback_start_ns]() {
    if (callback_start_ns != 0) {
      manager->RecordHolderInstallCallback(
          RecoveryProfileNowNs() - callback_start_ns);
    }
  };

  bool already_aborted = false;
  rpc::RecoveryManifest abort_manifest;
  {
    absl::MutexLock lock(&recovery_holder_admission_mutex_);
    already_aborted = state->aborted;
    if (already_aborted) {
      abort_manifest.CopyFrom(state->abort_manifest);
    }
  }

  if (already_aborted) {
    // The lower-rank failure may have raced with this install. If the candidate
    // stored the provisional lineage after the first cleanup, clean it again.
    if (status.ok() && install_reply.stored()) {
      SendRecoveryHolderRollback(state, abort_manifest);
    }
    return;
  }

  if (!status.ok() || !install_reply.stored() ||
      install_reply.reservation_id() != state->reservation_id) {
    AbortRecoveryHolderAdmissionSuffix(
        state,
        rpc::ReportRecoveryCandidateReply::NO_SLOT,
        state->latest_manifest);
    return;
  }

  {
    absl::MutexLock lock(&recovery_holder_admission_mutex_);
    if (state->aborted) {
      abort_manifest.CopyFrom(state->abort_manifest);
      already_aborted = true;
    } else {
      state->installed = true;
    }
  }

  if (already_aborted) {
    SendRecoveryHolderRollback(state, abort_manifest);
    return;
  }

  TryAdvanceRecoveryHolderAdmissions(state->task_id);
}

void CoreWorker::DispatchRecoveryHolderInstall(
    PreparedRecoveryHolderInstall prepared) {
  if (prepared.state == nullptr) {
    return;
  }

  auto state = prepared.state;
  auto candidate_client =
      core_worker_client_pool_->GetOrConnect(state->candidate_address);

  if (recovery_succession_profiling_enabled_) {
    state->install_start_ns = RecoveryProfileNowNs();
  }

  candidate_client->InstallRecoveryHolder(
      std::move(prepared.request),
      [this, state](const Status &status,
                    rpc::InstallRecoveryHolderReply &&install_reply) mutable {
        HandleRecoveryHolderInstallResult(
            state, status, std::move(install_reply));
      });
}

void CoreWorker::DispatchRecoveryHolderInstallBatch(
    std::vector<PreparedRecoveryHolderInstall> prepared) {
  if (prepared.empty()) {
    return;
  }

  size_t begin = 0;
  while (begin < prepared.size()) {
    size_t end = begin;
    uint64_t bytes = 0;

    while (end < prepared.size() &&
           end - begin < kRecoveryInstallBatchMaxItems) {
      const uint64_t next_bytes =
          static_cast<uint64_t>(prepared[end].request.ByteSizeLong());

      if (end > begin && bytes + next_bytes > kRecoveryInstallBatchMaxBytes) {
        break;
      }

      bytes += next_bytes;
      ++end;
    }

    // Always make progress even when one TaskSpec itself exceeds the byte cap.
    if (end == begin) {
      ++end;
    }

    if (end - begin == 1) {
      DispatchRecoveryHolderInstall(std::move(prepared[begin]));
      begin = end;
      continue;
    }

    rpc::InstallRecoveryHolderBatchRequest batch_request;
    batch_request.mutable_requests()->Reserve(static_cast<int>(end - begin));

    std::vector<std::shared_ptr<PendingRecoveryHolderAdmission>> states;
    states.reserve(end - begin);

    for (size_t i = begin; i < end; ++i) {
      states.push_back(prepared[i].state);
      batch_request.add_requests()->Swap(&prepared[i].request);
    }

    const rpc::Address candidate_address = states.front()->candidate_address;
    auto candidate_client = core_worker_client_pool_->GetOrConnect(candidate_address);

    const uint64_t install_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
    if (install_start_ns != 0) {
      for (const auto &state : states) {
        state->install_start_ns = install_start_ns;
      }
    }

    RAY_LOG(DEBUG) << "Patch 4E sending install batch with "
                   << states.size() << " logical installs and ~"
                   << bytes << " serialized request bytes";

    candidate_client->InstallRecoveryHolderBatch(
        std::move(batch_request),
        [this, states = std::move(states)](
            const Status &status,
            rpc::InstallRecoveryHolderBatchReply &&batch_reply) mutable {
          const bool shape_ok =
              status.ok() &&
              batch_reply.replies_size() == static_cast<int>(states.size());

          const Status item_status =
              shape_ok
                  ? Status::OK()
                  : (status.ok()
                         ? Status::IOError(
                               "InstallRecoveryHolderBatch reply size mismatch")
                         : status);

          for (size_t i = 0; i < states.size(); ++i) {
            rpc::InstallRecoveryHolderReply item_reply;
            if (shape_ok) {
              item_reply.CopyFrom(batch_reply.replies(static_cast<int>(i)));
            } else {
              item_reply.set_stored(false);
              item_reply.set_reservation_id(states[i]->reservation_id);
            }

            HandleRecoveryHolderInstallResult(
                states[i], item_status, std::move(item_reply));
          }
        });

    begin = end;
  }
}

void CoreWorker::HandleReportRecoveryCandidate(
    rpc::ReportRecoveryCandidateRequest request,
    rpc::ReportRecoveryCandidateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto prepared = PrepareRecoveryCandidateAdmission(
      request, reply, std::move(send_reply_callback));
  if (prepared.has_value()) {
    DispatchRecoveryHolderInstall(std::move(prepared.value()));
  }
}

void CoreWorker::HandleReportRecoveryCandidateBatch(
    rpc::ReportRecoveryCandidateBatchRequest request,
    rpc::ReportRecoveryCandidateBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (reply == nullptr) {
    send_reply_callback(Status::Invalid("null recovery candidate batch reply"),
                        nullptr,
                        nullptr);
    return;
  }

  const int count = request.requests_size();
  if (count == 0) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  reply->mutable_replies()->Reserve(count);
  std::vector<rpc::ReportRecoveryCandidateReply *> item_replies;
  item_replies.reserve(static_cast<size_t>(count));
  for (int i = 0; i < count; ++i) {
    item_replies.push_back(reply->add_replies());
  }

  struct CompletionState {
    absl::Mutex mutex;
    int remaining ABSL_GUARDED_BY(mutex);
    bool sent ABSL_GUARDED_BY(mutex) = false;
    Status first_error ABSL_GUARDED_BY(mutex) = Status::OK();
    rpc::SendReplyCallback callback;

    CompletionState(int count, rpc::SendReplyCallback cb)
        : remaining(count), callback(std::move(cb)) {}
  };

  auto completion =
      std::make_shared<CompletionState>(count, std::move(send_reply_callback));

  auto item_done = [completion](const Status &status, auto, auto) mutable {
    bool finish = false;
    Status final_status = Status::OK();

    {
      absl::MutexLock lock(&completion->mutex);
      if (!status.ok() && completion->first_error.ok()) {
        completion->first_error = status;
      }

      --completion->remaining;
      RAY_CHECK_GE(completion->remaining, 0);

      if (completion->remaining == 0 && !completion->sent) {
        completion->sent = true;
        final_status = completion->first_error;
        finish = true;
      }
    }

    if (finish) {
      completion->callback(final_status, nullptr, nullptr);
    }
  };

  // A batch normally comes from one candidate worker and one owner, but group
  // by candidate worker defensively before issuing holder-install batches.
  std::map<std::string, std::vector<PreparedRecoveryHolderInstall>> install_groups;

  for (int i = 0; i < count; ++i) {
    auto prepared = PrepareRecoveryCandidateAdmission(
        request.requests(i), item_replies[static_cast<size_t>(i)], item_done);

    if (!prepared.has_value()) {
      continue;
    }

    const std::string candidate_worker_id =
        prepared->state->candidate_address.worker_id();
    install_groups[candidate_worker_id].push_back(std::move(prepared.value()));
  }

  for (auto &[candidate_worker_id, group] : install_groups) {
    static_cast<void>(candidate_worker_id);
    DispatchRecoveryHolderInstallBatch(std::move(group));
  }
}

void CoreWorker::HandleInstallRecoveryHolder(
    rpc::InstallRecoveryHolderRequest request,
    rpc::InstallRecoveryHolderReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  reply->set_reservation_id(request.reservation_id());

  if (!recovery_succession_enabled_ ||
      recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    reply->set_stored(false);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const bool stored = recovery_succession_manager_->InstallRecoveryHolder(request);
  reply->set_stored(stored);

  if (stored) {
    RAY_LOG(INFO).WithField(TaskID::FromBinary(request.task_id()))
        << "Stored provisional recovery holder at rank "
        << request.proposed_rank();
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleInstallRecoveryHolderBatch(
    rpc::InstallRecoveryHolderBatchRequest request,
    rpc::InstallRecoveryHolderBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const int count = request.requests_size();
  reply->mutable_replies()->Reserve(count);

  const bool enabled =
      recovery_succession_enabled_ &&
      !recovery_witness_holder_baseline_enabled_ &&
      recovery_succession_manager_ != nullptr;

  for (int i = 0; i < count; ++i) {
    const rpc::InstallRecoveryHolderRequest &item_request = request.requests(i);
    rpc::InstallRecoveryHolderReply *item_reply = reply->add_replies();
    item_reply->set_reservation_id(item_request.reservation_id());

    const bool stored =
        enabled && recovery_succession_manager_->InstallRecoveryHolder(item_request);
    item_reply->set_stored(stored);

    if (stored) {
      RAY_LOG(DEBUG).WithField(TaskID::FromBinary(item_request.task_id()))
          << "Patch 4E batch stored provisional recovery holder at rank "
          << item_request.proposed_rank();
    }
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleCommitRecoveryManifest(
    rpc::CommitRecoveryManifestRequest request,
    rpc::CommitRecoveryManifestReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  static_cast<void>(reply);

  if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr &&
      request.has_manifest()) {
    const bool applied =
        recovery_succession_manager_->ApplyCommittedManifest(request.manifest());

    if (applied) {
      RAY_LOG(INFO).WithField(TaskID::FromBinary(request.manifest().task_id()))
          << "Applied committed recovery "
             "succession manifest";
    }
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleRecoverTaskOutput(rpc::RecoverTaskOutputRequest request,
                                         rpc::RecoverTaskOutputReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  if (!recovery_succession_enabled_ || recovery_succession_manager_ == nullptr) {
    reply->set_result(rpc::RecoverTaskOutputReply::DISABLED);

    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  rpc::TaskSpec replay_task_proto;
  rpc::RecoveryManifest latest_manifest;

  std::optional<TaskSpecification> owner_replay_task_spec;
  const rpc::TaskSpec *owner_replay_task_proto = nullptr;
  if (request.task_id().size() == TaskID::Size()) {
    owner_replay_task_spec =
        task_manager_->GetTaskSpec(TaskID::FromBinary(request.task_id()));
    if (owner_replay_task_spec.has_value()) {
      owner_replay_task_proto = &owner_replay_task_spec->GetMessage();
    }
  }

  const auto preparation = recovery_succession_manager_->PrepareTaskReplay(
      request,
      owner_replay_task_proto,
      &replay_task_proto,
      &latest_manifest);

  reply->mutable_latest_manifest()->CopyFrom(latest_manifest);

  using PreparationResult = RecoverySuccessionManager::ReplayPreparationResult;

  if (preparation ==
      PreparationResult::WITNESS_CONFIRMATION_REQUIRED) {
    // Do not trust the recovery requester's cached manifest as proof of
    // commitment. This provisional holder independently queries the compact
    // witnesses stored in its own manifest.
    rpc::RecoveryManifest provisional_manifest;
    provisional_manifest.CopyFrom(latest_manifest);

    // Adaptive Recovery Frontier witnesses store the shared topology under
    // the group/leader TaskID, while replay requests remain task-centric.
    // Query the shared witness key, but keep requested_task_id so the manager
    // can translate the verified group manifest back to this member.
    const TaskID requested_task_id = TaskID::FromBinary(request.task_id());
    rpc::RecoveryManifest witness_lookup_manifest;
    witness_lookup_manifest.CopyFrom(provisional_manifest);
    std::string expected_witness_task_id = request.task_id();
    const auto frontier_membership =
        recovery_succession_manager_->GetRecoveryFrontierMembership(
            requested_task_id);
    if (frontier_membership.has_value()) {
      expected_witness_task_id = frontier_membership->group_id.Binary();
      witness_lookup_manifest.set_task_id(expected_witness_task_id);
    }

    // TEST ONLY: deterministically make the holder's own witness
    // confirmation unavailable. The requester may still have obtained the
    // holder manifest from a real witness; this hook verifies that the
    // requester alone cannot vouch for a provisional holder.
    if (RayConfig::instance()
            .recovery_succession_test_fail_holder_witness_confirmation()) {
      RAY_LOG(WARNING)
          .WithField(TaskID::FromBinary(request.task_id()))
          << "TEST ONLY: suppressing provisional holder witness confirmation";

      reply->set_result(
          rpc::RecoverTaskOutputReply::TASK_NOT_FOUND);

      send_reply_callback(
          Status::OK(),
          nullptr,
          nullptr);
      return;
    }

    LookupRecoveryManifestFromWitnesses(
        witness_lookup_manifest,
        [this,
        request = std::move(request),
        requested_task_id,
        expected_witness_task_id = std::move(expected_witness_task_id),
        reply,
        send_reply_callback = std::move(send_reply_callback)](
            std::optional<rpc::RecoveryManifest> witness_manifest) mutable {
          if (!witness_manifest.has_value() ||
              witness_manifest->task_id() != expected_witness_task_id ||
              !witness_manifest->has_version()) {
            reply->set_result(
                rpc::RecoverTaskOutputReply::TASK_NOT_FOUND);

            send_reply_callback(
                Status::OK(),
                nullptr,
                nullptr);
            return;
          }

          if (witness_manifest->tombstoned()) {
            recovery_succession_manager_->ApplyRecoveryTombstone(
                witness_manifest.value());

            reply->mutable_latest_manifest()->CopyFrom(
                witness_manifest.value());

            reply->set_result(
                rpc::RecoverTaskOutputReply::TOMBSTONED);

            send_reply_callback(
                Status::OK(),
                nullptr,
                nullptr);
            return;
          }

          rpc::RecoveryManifest confirmed_manifest;

          if (!recovery_succession_manager_
                  ->ConfirmProvisionalHolderFromWitness(
                      requested_task_id,
                      witness_manifest.value(),
                      &confirmed_manifest)) {
            reply->mutable_latest_manifest()->CopyFrom(
                witness_manifest.value());

            reply->set_result(
                rpc::RecoverTaskOutputReply::TASK_NOT_FOUND);

            send_reply_callback(
                Status::OK(),
                nullptr,
                nullptr);
            return;
          }

          // Re-run ordinary replay preparation using the manifest that this
          // holder itself verified from a witness.
          request.mutable_requester_manifest()->CopyFrom(
              confirmed_manifest);

          RAY_LOG(INFO)
              .WithField(
                  TaskID::FromBinary(request.task_id()))
              << "Promoted provisional recovery holder "
                "from witness-backed manifest";

          HandleRecoverTaskOutput(
              std::move(request),
              reply,
              std::move(send_reply_callback));
        });

    return;
  }


  switch (preparation) {

  case PreparationResult::WITNESS_CONFIRMATION_REQUIRED:
    // Handled asynchronously above.
    return;

  case PreparationResult::TASK_NOT_FOUND:
  case PreparationResult::WRONG_HOLDER:
    reply->set_result(rpc::RecoverTaskOutputReply::TASK_NOT_FOUND);
    break;

  case PreparationResult::MANIFEST_STALE:
    reply->set_result(rpc::RecoverTaskOutputReply::MANIFEST_STALE);
    break;

  case PreparationResult::TOMBSTONED:
    reply->set_result(rpc::RecoverTaskOutputReply::TOMBSTONED);
    break;

  case PreparationResult::RETRY_LIMIT_EXCEEDED:
    reply->set_result(rpc::RecoverTaskOutputReply::RETRY_LIMIT_EXCEEDED);
    break;

  case PreparationResult::ALREADY_OWNED: {
    // The first request already registered this task's replay on this worker.
    // Return its existing deterministic reference without consuming a retry,
    // touching reference counts, or submitting a second execution.
    TaskSpecification existing_task(std::move(replay_task_proto));
    if (request.return_index() >= existing_task.NumReturns()) {
      reply->set_result(rpc::RecoverTaskOutputReply::REPLAY_FAILED);
      break;
    }
    const ObjectID return_id = existing_task.ReturnId(request.return_index());
    if (!reference_counter_->OwnedByUs(return_id)) {
      // The result may have gone out of scope since replay registration.
      // Do not recreate a task from a stale recovery request.
      reply->set_result(rpc::RecoverTaskOutputReply::REPLAY_FAILED);
      break;
    }
    auto *ref = reply->mutable_replacement_ref();
    ref->set_object_id(return_id.Binary());
    ref->mutable_owner_address()->CopyFrom(rpc_address_);
    ref->set_call_site("existing recovery replay");
    auto *metadata = ref->mutable_recovery_metadata();
    metadata->set_task_id(request.task_id());
    metadata->set_return_index(request.return_index());
    metadata->mutable_manifest()->CopyFrom(latest_manifest);
    if (existing_task.TensorTransport().has_value()) {
      ref->set_tensor_transport(*existing_task.TensorTransport());
    }
    reply->set_result(rpc::RecoverTaskOutputReply::RECOVERED);
    RAY_LOG(INFO).WithField(return_id)
        << "Reusing locally owned recovery return";
    break;
  }

  case PreparationResult::READY:
    break;
  }

  if (preparation != PreparationResult::READY) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  auto replacement_ref =
      StartRecoveryReplay(
          std::move(replay_task_proto),
          request.return_index());

  if (!replacement_ref.has_value()) {
    reply->set_result(
        rpc::RecoverTaskOutputReply::REPLAY_FAILED);

    send_reply_callback(
        Status::OK(),
        nullptr,
        nullptr);
    return;
  }

  reply->set_result(
      rpc::RecoverTaskOutputReply::RECOVERED);

  reply->mutable_replacement_ref()->CopyFrom(
      replacement_ref.value());

  RAY_LOG(INFO)
      .WithField(
          TaskID::FromBinary(request.task_id()))
      << "Recovery succession replay accepted for return "
      << request.return_index();

  send_reply_callback(
      Status::OK(),
      nullptr,
      nullptr);
  
}



std::optional<rpc::ObjectReference>
CoreWorker::StartRecoveryReplay(
    rpc::TaskSpec replay_task_proto,
    uint32_t return_index) {
  if (!recovery_succession_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    return std::nullopt;
  }

  // The worker performing the replay becomes the new owner.
  replay_task_proto.mutable_caller_address()->CopyFrom(
      rpc_address_);

  // Recovery must not remain pinned to a dead/preferred node when
  // the original task used soft node affinity.
  auto *scheduling_strategy =
      replay_task_proto.mutable_scheduling_strategy();

  if (scheduling_strategy
          ->has_node_affinity_scheduling_strategy() &&
      scheduling_strategy
          ->node_affinity_scheduling_strategy()
          .soft()) {
    scheduling_strategy->clear_scheduling_strategy();
    scheduling_strategy->mutable_default_scheduling_strategy();

    RAY_LOG(INFO)
        .WithField(
            TaskID::FromBinary(
                replay_task_proto.task_id()))
        << "Cleared soft node affinity for recovery replay";
  }

  // Same deterministic task, new execution attempt.
  replay_task_proto.set_attempt_number(
      replay_task_proto.attempt_number() + 1);

  RAY_LOG(INFO)
      .WithField(
          TaskID::FromBinary(
              replay_task_proto.task_id()))
      << "Preparing recovery replay attempt "
      << replay_task_proto.attempt_number();

  TaskSpecification replay_task(
      std::move(replay_task_proto));

  // Restore recovery information for upstream dependencies.
  for (size_t i = 0; i < replay_task.NumArgs(); ++i) {
    if (!replay_task.ArgByRef(i)) {
      continue;
    }

    const auto &arg_ref = replay_task.ArgRef(i);

    if (arg_ref.object_id().empty() ||
        !arg_ref.has_recovery_metadata()) {
      continue;
    }

    const ObjectID dependency_id =
        ObjectID::FromBinary(
            arg_ref.object_id());

    recovery_succession_manager_->RegisterBorrowedObject(
        dependency_id,
        arg_ref.recovery_metadata());

    RAY_LOG(INFO)
        .WithField(replay_task.TaskId())
        .WithField(dependency_id)
        << "Registered recovery metadata for "
           "replay task dependency";
  }

  const int32_t max_retries =
      replay_task.MaxRetries();

  std::vector<rpc::ObjectReference> returned_refs =
      task_manager_->AddPendingTaskForRecovery(
          rpc_address_,
          replay_task,
          "recovery replay",
          max_retries);

  if (return_index >= returned_refs.size()) {
    return std::nullopt;
  }

  // Remove stale OWNER_DIED values for deterministic return IDs.
  std::vector<ObjectID> stale_owner_died_returns;

  for (const auto &return_ref : returned_refs) {
    if (return_ref.object_id().empty()) {
      continue;
    }

    const ObjectID return_id =
        ObjectID::FromBinary(
            return_ref.object_id());

    const auto existing =
        memory_store_->GetIfExists(return_id);

    if (existing == nullptr) {
      continue;
    }

    rpc::ErrorType error_type;

    if (existing->IsException(&error_type) &&
        error_type ==
            rpc::ErrorType::OWNER_DIED) {
      stale_owner_died_returns.push_back(
          return_id);
    }
  }

  if (!stale_owner_died_returns.empty()) {
    memory_store_->Delete(
        stale_owner_died_returns);

    for (const auto &return_id :
         stale_owner_died_returns) {
      RAY_LOG(INFO)
          .WithField(return_id)
          << "Removed stale local OWNER_DIED "
             "before recovery replay";
    }
  }

  recovery_succession_manager_->RegisterOwnedTask(
      replay_task,
      &returned_refs);

  rpc::ObjectReference replacement_ref;
  replacement_ref.CopyFrom(
      returned_refs[return_index]);

  io_service_.post(
      [this,
       replay_task = std::move(replay_task)]() mutable {
        const TaskID replay_task_id =
            replay_task.TaskId();

        RAY_LOG(INFO)
            .WithField(replay_task_id)
            << "Submitting recovery replay";

        normal_task_submitter_->SubmitTask(
            std::move(replay_task));
      },
      "CoreWorker.StartRecoveryReplay");

  return replacement_ref;
}


void CoreWorker::TryRecoverTaskDependency(
    const ObjectID &object_id,
    std::function<void(bool)> callback) {
  if (!recovery_succession_enabled_ ||
      recovery_succession_manager_ ==
          nullptr) {
    callback(false);
    return;
  }

  RAY_LOG(INFO)
      .WithField(object_id)
      << "Trying recovery succession for "
         "normal-task dependency";

  RecoverBorrowedObject(
      object_id,
      std::move(callback));
}


void CoreWorker::RecoverBorrowedObject(const ObjectID &object_id,
                                       RecoveryAttemptCallback callback) {
  if (!recovery_succession_enabled_ || recovery_succession_manager_ == nullptr) {
    callback(false);
    return;
  }

  RecoverySuccessionManager::BorrowedObjectRecoveryPlan plan;

  if (!recovery_succession_manager_->GetBorrowedObjectRecoveryPlan(object_id, &plan)) {
    callback(false);
    return;
  }

  // The witness-as-holder baseline has its own recovery path.
  if (recovery_succession_enabled_ &&
      recovery_witness_holder_baseline_enabled_) {
    TryRecoveryWitnessHolders(
        object_id,
        plan.return_index,
        plan.cached_manifest,
        0,
        std::move(callback));
    return;
  }



  // Try the cached fixed manifest first. Witnesses are only
  // consulted if every cached entry is exhausted.
  TryRecoveryHolders(
      object_id, plan.return_index, plan.cached_manifest, 0, false, std::move(callback));
}

void CoreWorker::TryRecoveryHolders(const ObjectID &object_id,
                                    uint32_t return_index,
                                    const rpc::RecoveryManifest &manifest,
                                    size_t holder_index,
                                    bool witness_lookup_attempted,
                                    RecoveryAttemptCallback callback) {
  if (manifest.tombstoned()) {
    callback(false);
    return;
  }

  if (holder_index >= static_cast<size_t>(manifest.succession_size())) {
    if (witness_lookup_attempted) {
      callback(false);
      return;
    }

    // A recipe piggyback contains the owner's pre-admission view, so there
    // may be no cached non-owner holder to contact yet. Frontier witnesses
    // index topology by group/leader ID, while replay remains member-keyed.
    // Use locally installed membership to select the witness key.
    rpc::RecoveryManifest witness_lookup_manifest;
    witness_lookup_manifest.CopyFrom(manifest);
    const auto membership =
        recovery_succession_manager_->GetRecoveryFrontierMembership(
            object_id.TaskId());
    const bool frontier_lookup =
        membership.has_value() &&
        RayConfig::instance().recovery_frontier_group_size() > 1 &&
        !RayConfig::instance().enable_recovery_succession_certificate_admission();
    if (frontier_lookup) {
      if (manifest.task_id() != object_id.TaskId().Binary()) {
        callback(false);
        return;
      }
      witness_lookup_manifest.set_task_id(membership->group_id.Binary());
    }
    const std::string expected_witness_task_id = witness_lookup_manifest.task_id();

    LookupRecoveryManifestFromWitnesses(
        witness_lookup_manifest,
        [this, object_id, return_index, manifest, frontier_lookup,
         expected_witness_task_id, callback = std::move(callback)](
            std::optional<rpc::RecoveryManifest> witness_manifest) mutable {
          if (!witness_manifest.has_value() ||
              !witness_manifest->has_version() ||
              witness_manifest->task_id() != expected_witness_task_id ||
              CompareRecoveryManifestVersions(witness_manifest.value(), manifest) <= 0) {
            callback(false);
            return;
          }

          if (frontier_lookup && witness_manifest->tombstoned()) {
            // Apply the original group-keyed tombstone to every local member
            // before discarding the recipe/membership needed for translation.
            recovery_succession_manager_->ApplyRecoveryTombstone(
                witness_manifest.value());
            callback(false);
            return;
          }

          rpc::RecoveryManifest newer_manifest;
          newer_manifest.CopyFrom(witness_manifest.value());
          if (frontier_lookup) {
            // Match BuildFrontierMemberManifest: topology/version come from
            // witnesses, but task identity and retry budget belong to the
            // requested member, not the group's leader.
            newer_manifest.set_task_id(manifest.task_id());
            newer_manifest.set_job_id(manifest.job_id());
            newer_manifest.set_max_recovery_attempts(manifest.max_recovery_attempts());
          }

          // Updating a borrowed view does not commit provisional holder state
          // on the ordinary ordered-admission path. Even a self-directed replay
          // RPC must still pass HandleRecoverTaskOutput's independent witness
          // confirmation; this requester lookup is only holder discovery.
          recovery_succession_manager_->UpdateBorrowedObjectManifest(object_id,
                                                                     newer_manifest);

          if (newer_manifest.tombstoned()) {
            callback(false);
            return;
          }

          TryRecoveryHolders(
              object_id, return_index, newer_manifest, 0, true, std::move(callback));
        });

    return;
  }

  const rpc::RecoveryHolder &holder = manifest.succession(static_cast<int>(holder_index));

  // Rank zero is the failed original owner.
  if (holder.rank() == 0) {
    TryRecoveryHolders(object_id,
                       return_index,
                       manifest,
                       holder_index + 1,
                       witness_lookup_attempted,
                       std::move(callback));
    return;
  }

  if (recovery_succession_manager_->IsRecoveryHolderKnownFailed(holder)) {
    RAY_LOG(INFO).WithField(object_id)
        << "Skipping known-dead recovery holder rank " << holder.rank();

    TryRecoveryHolders(object_id,
                       return_index,
                       manifest,
                       holder_index + 1,
                       witness_lookup_attempted,
                       std::move(callback));
    return;
  }

  rpc::RecoverTaskOutputRequest request;
  request.set_task_id(manifest.task_id());
  request.set_return_index(return_index);
  request.mutable_requester_manifest()->CopyFrom(manifest);

  auto holder_client = core_worker_client_pool_->GetOrConnect(holder.address());

  const uint32_t holder_rank = holder.rank();

  holder_client->RecoverTaskOutput(
      std::move(request),
      [this,
       object_id,
       return_index,
       manifest,
       holder_index,
       holder_rank,
       witness_lookup_attempted,
       callback = std::move(callback)](const Status &status,
                                       rpc::RecoverTaskOutputReply &&reply) mutable {
        if (!status.ok()) {
          TryRecoveryHolders(object_id,
                             return_index,
                             manifest,
                             holder_index + 1,
                             witness_lookup_attempted,
                             std::move(callback));
          return;
        }

        if (reply.result() == rpc::RecoverTaskOutputReply::MANIFEST_STALE &&
            reply.has_latest_manifest()) {
          recovery_succession_manager_->UpdateBorrowedObjectManifest(
              object_id, reply.latest_manifest());

          TryRecoveryHolders(object_id,
                             return_index,
                             reply.latest_manifest(),
                             0,
                             witness_lookup_attempted,
                             std::move(callback));
          return;
        }

        if (reply.result() == rpc::RecoverTaskOutputReply::TOMBSTONED) {
          if (reply.has_latest_manifest()) {
            recovery_succession_manager_->UpdateBorrowedObjectManifest(
                object_id, reply.latest_manifest());
          }

          callback(false);
          return;
        }

        if (reply.result() != rpc::RecoverTaskOutputReply::RECOVERED) {
          TryRecoveryHolders(object_id,
                             return_index,
                             manifest,
                             holder_index + 1,
                             witness_lookup_attempted,
                             std::move(callback));
          return;
        }

        if (!reply.has_replacement_ref()) {
          TryRecoveryHolders(object_id,
                             return_index,
                             manifest,
                             holder_index + 1,
                             witness_lookup_attempted,
                             std::move(callback));
          return;
        }

        const rpc::ObjectReference &replacement_ref = reply.replacement_ref();

        if (replacement_ref.object_id() != object_id.Binary() ||
            replacement_ref.owner_address().worker_id().size() != WorkerID::Size()) {
          TryRecoveryHolders(object_id,
                             return_index,
                             manifest,
                             holder_index + 1,
                             witness_lookup_attempted,
                             std::move(callback));
          return;
        }

        // Transition the requester from the failed original owner to
        // the acting recovery holder.
        reference_counter_->AddBorrowedObject(
            object_id,
            ObjectID::Nil(),
            replacement_ref.owner_address());

        if (replacement_ref.has_recovery_metadata()) {
          recovery_succession_manager_->RegisterBorrowedObject(
              object_id,
              replacement_ref.recovery_metadata());
        }

        // The original FutureResolver stopped resolving this object when
        // the original owner died. This matters when the object had never
        // been materialized before the failure (in-flight task recovery).
        //
        // Re-arm resolution against the acting holder so that when the
        // replay finishes, the requester learns that the replacement
        // object has been created / placed in plasma.
        if (future_resolver_ != nullptr) {
          future_resolver_->ResolveFutureAsync(
              object_id,
              replacement_ref.owner_address());
        }

        RAY_LOG(INFO).WithField(object_id)
            << "Recovery succession accepted by holder rank "
            << holder_rank
            << "; future resolution restarted against acting holder";

        callback(true);
      });
}




void CoreWorker::TryRecoveryWitnessHolders(
    const ObjectID &object_id,
    uint32_t return_index,
    const rpc::RecoveryManifest &manifest,
    size_t witness_index,
    RecoveryAttemptCallback callback) {
  if (!recovery_succession_enabled_ ||
      !recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    callback(false);
    return;
  }

  if (manifest.tombstoned()) {
    callback(false);
    return;
  }

  if (witness_index >=
      static_cast<size_t>(
          manifest.witness_raylets_size())) {
    callback(false);
    return;
  }

  const rpc::Address &witness =
      manifest.witness_raylets(
          static_cast<int>(witness_index));

  rpc::GetRecoveryWitnessRequest request;
  request.set_task_id(manifest.task_id());

  request.set_claim_recovery(true);
  request.mutable_claimant_address()->CopyFrom(
      rpc_address_);

  auto witness_client =
      raylet_client_pool_->GetOrConnectByAddress(
          witness);

  witness_client->GetRecoveryWitness(
      std::move(request),
      [this,
       object_id,
       return_index,
       manifest,
       witness_index,
       callback = std::move(callback)](
          const Status &status,
          rpc::GetRecoveryWitnessReply &&reply) mutable {
        // Witness unavailable or it does not contain full lineage:
        // try the next preassigned witness.
        if (!status.ok() ||
            !reply.found() ||
            !reply.has_manifest()) {
          TryRecoveryWitnessHolders(
              object_id,
              return_index,
              manifest,
              witness_index + 1,
              std::move(callback));
          return;
        }

        if (reply.claim_result() ==
            rpc::GetRecoveryWitnessReply::
                CLAIM_TOMBSTONED) {
          recovery_succession_manager_
              ->UpdateBorrowedObjectManifest(
                  object_id,
                  reply.manifest());

          callback(false);
          return;
        }

        if (reply.claim_result() ==
            rpc::GetRecoveryWitnessReply::
                CLAIM_RETRY_LIMIT_EXCEEDED) {
          callback(false);
          return;
        }


        if (reply.claim_result() ==
            rpc::GetRecoveryWitnessReply::
                CLAIM_ALREADY_GRANTED) {
          if (!reply.has_acting_owner() ||
              reply.acting_owner()
                      .worker_id()
                      .size() != WorkerID::Size()) {
            callback(false);
            return;
          }

          rpc::RecoveryObjectMetadata metadata;
          metadata.set_task_id(
              reply.manifest().task_id());
          metadata.set_return_index(
              return_index);
          metadata.mutable_manifest()->CopyFrom(
              reply.manifest());

          // Another borrower already won the atomic replay claim.
          // Follow that worker as the acting owner instead of replaying.
          reference_counter_->AddBorrowedObject(
              object_id,
              ObjectID::Nil(),
              reply.acting_owner());

          recovery_succession_manager_
              ->RegisterBorrowedObject(
                  object_id,
                  metadata);

          if (future_resolver_ != nullptr) {
            future_resolver_->ResolveFutureAsync(
                object_id,
                reply.acting_owner());
          }

          RAY_LOG(INFO)
              .WithField(object_id)
              << "Witness-holder baseline recovery already "
                "claimed by another acting owner";

          callback(true);
          return;
        }


        if (reply.claim_result() !=
                rpc::GetRecoveryWitnessReply::CLAIM_GRANTED ||
            !reply.has_task_spec() ||
            !reply.has_acting_owner()) {
          TryRecoveryWitnessHolders(
              object_id,
              return_index,
              manifest,
              witness_index + 1,
              std::move(callback));
          return;
        }

        if (reply.acting_owner().worker_id() !=
                rpc_address_.worker_id() ||
            reply.acting_owner().node_id() !=
                rpc_address_.node_id()) {
          callback(false);
          return;
        }



        if (reply.manifest().task_id() !=
                manifest.task_id() ||
            reply.task_spec().task_id() !=
                manifest.task_id()) {
          TryRecoveryWitnessHolders(
              object_id,
              return_index,
              manifest,
              witness_index + 1,
              std::move(callback));
          return;
        }

        if (reply.manifest().tombstoned()) {
          recovery_succession_manager_
              ->UpdateBorrowedObjectManifest(
                  object_id,
                  reply.manifest());

          callback(false);
          return;
        }

        if (!reply.task_spec()
                 .has_recovery_manifest()) {
          TryRecoveryWitnessHolders(
              object_id,
              return_index,
              manifest,
              witness_index + 1,
              std::move(callback));
          return;
        }

        rpc::TaskSpec replay_task_proto;
        replay_task_proto.CopyFrom(
            reply.task_spec());

        // Always replay using the witness's latest retained manifest.
        replay_task_proto
            .mutable_recovery_manifest()
            ->CopyFrom(reply.manifest());

        


        auto replacement_ref =
            StartRecoveryReplay(
                std::move(replay_task_proto),
                return_index);

        if (!replacement_ref.has_value()) {
          callback(false);
          return;
        }

        const rpc::ObjectReference &replacement =
            replacement_ref.value();

        if (replacement.object_id() !=
            object_id.Binary()) {
          callback(false);
          return;
        }

        // This requesting CoreWorker is the acting owner for the
        // baseline replay.
        reference_counter_->AddBorrowedObject(
            object_id,
            ObjectID::Nil(),
            replacement.owner_address());

        if (replacement.has_recovery_metadata()) {
          recovery_succession_manager_
              ->RegisterBorrowedObject(
                  object_id,
                  replacement.recovery_metadata());
        }

        if (future_resolver_ != nullptr) {
          future_resolver_->ResolveFutureAsync(
              object_id,
              replacement.owner_address());
        }

        RAY_LOG(INFO)
            .WithField(object_id)
            << "Witness-as-holder baseline recovery "
               "accepted using witness index "
            << witness_index;

        callback(true);
      });


      
}

void CoreWorker::HandleApplyRecoveryTombstone(
    rpc::ApplyRecoveryTombstoneRequest request,
    rpc::ApplyRecoveryTombstoneReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  static_cast<void>(reply);

  if (!recovery_succession_enabled_ || recovery_succession_manager_ == nullptr ||
      !request.has_tombstone()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const rpc::RecoveryManifest &tombstone = request.tombstone();

  const bool applied = recovery_succession_manager_->ApplyRecoveryTombstone(tombstone);

  if (applied) {
    RAY_LOG(INFO).WithField(TaskID::FromBinary(tombstone.task_id()))
        << "Applied recovery succession tombstone";
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::PublishRecoveryTombstone(rpc::RecoveryManifest tombstone) {
  const TaskID task_id = TaskID::FromBinary(tombstone.task_id());

  // Keep a separate copy for the asynchronous callback.
  // Do not move the same object that is passed as the RPC manifest.
  rpc::RecoveryManifest callback_tombstone;
  callback_tombstone.CopyFrom(tombstone);

  PublishRecoveryManifestToWitnesses(
      tombstone,
      [this, task_id, tombstone = std::move(callback_tombstone)](
          bool stored, std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (!stored) {
          RAY_LOG(WARNING).WithField(task_id) << "Failed to publish recovery succession "
                                              << "tombstone to witnesses";

          if (newer_manifest.has_value()) {
            RAY_LOG(WARNING).WithField(task_id)
                << "Witness returned newer recovery manifest: "
                << "generation=" << newer_manifest->version().generation()
                << ", tombstoned=" << newer_manifest->tombstoned();
          }

          if (newer_manifest.has_value() && newer_manifest->tombstoned()) {
            const bool applied_newer =
                recovery_succession_manager_->ApplyRecoveryTombstone(
                    newer_manifest.value());

            if (applied_newer) {
              RAY_LOG(INFO).WithField(task_id) << "Applied newer recovery succession "
                                               << "tombstone returned by witness";
            }
          }

          recovery_tombstones_in_flight_.erase(task_id);
          return;
        }

        const bool applied =
            recovery_succession_manager_->ApplyRecoveryTombstone(tombstone);

        if (!applied) {
          RAY_LOG(WARNING).WithField(task_id) << "Witness stored recovery tombstone, "
                                              << "but local tombstone application failed";

          recovery_tombstones_in_flight_.erase(task_id);
          return;
        }

        RAY_LOG(INFO).WithField(task_id) << "Published recovery succession tombstone";

        PropagateRecoveryTombstoneToHolders(tombstone);

        recovery_tombstones_in_flight_.erase(task_id);
      });
}

void CoreWorker::PropagateRecoveryTombstoneToHolders(
    const rpc::RecoveryManifest &tombstone) {
  const TaskID task_id = TaskID::FromBinary(tombstone.task_id());

  for (const rpc::RecoveryHolder &holder : tombstone.succession()) {
    if (holder.address().worker_id() == rpc_address_.worker_id()) {
      continue;
    }

    rpc::ApplyRecoveryTombstoneRequest request;
    request.mutable_tombstone()->CopyFrom(tombstone);

    const uint32_t holder_rank = holder.rank();

    auto holder_client = core_worker_client_pool_->GetOrConnect(holder.address());

    holder_client->ApplyRecoveryTombstone(
        std::move(request),
        [task_id, holder_rank](const Status &status,
                               rpc::ApplyRecoveryTombstoneReply &&reply) {
          static_cast<void>(reply);

          if (!status.ok()) {
            RAY_LOG(WARNING).WithField(task_id)
                << "Failed to send recovery "
                << "tombstone to rank " << holder_rank << ": " << status;
          }
        });
  }
}

void CoreWorker::HandleWaitForActorRefDeleted(
    rpc::WaitForActorRefDeletedRequest request,
    rpc::WaitForActorRefDeletedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto actor_id = ActorID::FromBinary(request.actor_id());

  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Send a response to trigger cleaning up the actor state once the handle is
  // no longer in scope.
  auto respond = [send_reply_callback](const ActorID &respond_actor_id) {
    RAY_LOG(DEBUG).WithField(respond_actor_id)
        << "Replying to HandleWaitForActorRefDeleted";
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  /// The callback for each request is stored in the reference counter due to retries
  /// and message reordering where the callback of the retry of the request could be
  /// overwritten by the callback of the initial request.
  if (actor_creator_->IsActorInRegistering(actor_id)) {
    actor_creator_->AsyncWaitForActorRegisterFinish(
        actor_id, [this, actor_id, respond = std::move(respond)](const auto &status) {
          if (!status.ok()) {
            respond(actor_id);
          } else {
            RAY_LOG(DEBUG).WithField(actor_id) << "Received HandleWaitForActorRefDeleted";
            actor_manager_->WaitForActorRefDeleted(actor_id, std::move(respond));
          }
        });
  } else {
    RAY_LOG(DEBUG).WithField(actor_id) << "Received HandleWaitForActorRefDeleted";
    actor_manager_->WaitForActorRefDeleted(actor_id, std::move(respond));
  }
}

StatusSet<StatusT::InvalidArgument> CoreWorker::ProcessSubscribeMessage(
    const rpc::SubMessage &sub_message,
    rpc::ChannelType channel_type,
    const std::string &key_id,
    const NodeID &subscriber_id) {
  StatusSet<StatusT::InvalidArgument> result =
      object_info_publisher_->RegisterSubscription(channel_type, subscriber_id, key_id);
  if (result.has_error()) {
    return result;
  }

  if (!sub_message.has_worker_ref_removed_message() &&
      !sub_message.has_worker_object_locations_message()) {
    return StatusT::InvalidArgument(
        absl::StrFormat("Unexpected subscribe command has been received: %s"
                        "Expected worker_ref_removed or "
                        "worker_object_locations message",
                        sub_message.DebugString()));
  }

  if (sub_message.has_worker_ref_removed_message()) {
    ProcessSubscribeForRefRemoved(sub_message.worker_ref_removed_message());
  } else {  // worker_object_locations_message case
    ProcessSubscribeObjectLocations(sub_message.worker_object_locations_message());
  }
  return StatusT::OK();
}

void CoreWorker::HandlePubsubLongPolling(rpc::PubsubLongPollingRequest request,
                                         rpc::PubsubLongPollingReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  const NodeID subscriber_id = NodeID::FromBinary(request.subscriber_id());
  RAY_LOG(DEBUG).WithField(subscriber_id) << "Got a long polling request from a node";
  object_info_publisher_->ConnectToSubscriber(request,
                                              reply->mutable_publisher_id(),
                                              reply->mutable_pub_messages(),
                                              std::move(send_reply_callback));
}

void CoreWorker::HandlePubsubCommandBatch(rpc::PubsubCommandBatchRequest request,
                                          rpc::PubsubCommandBatchReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  const NodeID subscriber_id = NodeID::FromBinary(request.subscriber_id());
  for (const auto &command : request.commands()) {
    if (!command.has_unsubscribe_message() && !command.has_subscribe_message()) {
      send_reply_callback(Status::InvalidArgument(absl::StrFormat(
                              "Unexpected pubsub command has been received: %s."
                              "Expected either unsubscribe or subscribe message",
                              command.DebugString())),
                          nullptr,
                          nullptr);
      return;
    }

    if (command.has_unsubscribe_message()) {
      object_info_publisher_->UnregisterSubscription(
          command.channel_type(), subscriber_id, command.key_id());
    } else {  // subscribe_message case
      StatusSet<StatusT::InvalidArgument> result =
          ProcessSubscribeMessage(command.subscribe_message(),
                                  command.channel_type(),
                                  command.key_id(),
                                  subscriber_id);
      if (result.has_error()) {
        // Terminate the worker if the subscribe message is invalid.
        send_reply_callback(
            Status::InvalidArgument(
                std::get<StatusT::InvalidArgument>(result.error()).message()),
            nullptr,
            nullptr);
        return;
      }
    }
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleUpdateObjectLocationBatch(
    rpc::UpdateObjectLocationBatchRequest request,
    rpc::UpdateObjectLocationBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &worker_id = request.intended_worker_id();
  if (HandleWrongRecipient(WorkerID::FromBinary(worker_id), send_reply_callback)) {
    return;
  }
  const auto &node_id = NodeID::FromBinary(request.node_id());
  const auto &object_location_updates = request.object_location_updates();

  for (const auto &object_location_update : object_location_updates) {
    const auto &object_id = ObjectID::FromBinary(object_location_update.object_id());

    if (object_location_update.has_spilled_location_update()) {
      AddSpilledObjectLocationOwner(
          object_id,
          object_location_update.spilled_location_update().spilled_url(),
          object_location_update.spilled_location_update().spilled_to_local_storage()
              ? node_id
              : NodeID::Nil(),
          object_location_update.has_generator_id()
              ? std::optional<ObjectID>(
                    ObjectID::FromBinary(object_location_update.generator_id()))
              : std::nullopt);
    }

    if (object_location_update.has_plasma_location_update()) {
      if (object_location_update.plasma_location_update() ==
          rpc::ObjectPlasmaLocationUpdate::ADDED) {
        AddObjectLocationOwner(object_id, node_id);
      } else if (object_location_update.plasma_location_update() ==
                 rpc::ObjectPlasmaLocationUpdate::REMOVED) {
        RemoveObjectLocationOwner(object_id, node_id);
      } else {
        RAY_LOG(FATAL) << "Invalid object plasma location update "
                       << object_location_update.plasma_location_update()
                       << " has been received.";
      }
    }
  }

  send_reply_callback(Status::OK(),
                      /*success_callback_on_reply=*/nullptr,
                      /*failure_callback_on_reply=*/nullptr);
}

void CoreWorker::AddSpilledObjectLocationOwner(
    const ObjectID &object_id,
    const std::string &spilled_url,
    const NodeID &spilled_node_id,
    const std::optional<ObjectID> &generator_id) {
  RAY_LOG(DEBUG).WithField(object_id).WithField(spilled_node_id)
      << "Received object spilled location update for object, which has been spilled "
         "to "
      << spilled_url << " on node";
  if (generator_id.has_value()) {
    // For dynamically generated return values, the raylet may spill the
    // primary copy before we know about the object. This can happen when the
    // object is spilled before the reply from the task that created the
    // object. Add the dynamically created object to our ref counter so that we
    // know that it exists.
    if (task_manager_->ObjectRefStreamExists(*generator_id)) {
      // ObjectRefStreamExists is used to distinguigsh num_returns="dynamic" vs
      // "streaming".
      task_manager_->TemporarilyOwnGeneratorReturnRefIfNeeded(object_id, *generator_id);
    } else {
      reference_counter_->AddDynamicReturn(object_id, *generator_id);
    }
  }

  auto reference_exists =
      reference_counter_->HandleObjectSpilled(object_id, spilled_url, spilled_node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object not found";
  }
}

void CoreWorker::AddObjectLocationOwner(const ObjectID &object_id,
                                        const NodeID &node_id) {
  if (gcs_client_->Nodes().IsNodeDead(node_id)) {
    RAY_LOG(DEBUG).WithField(node_id).WithField(object_id)
        << "Attempting to add object location for a dead node. Ignoring this request.";
    return;
  }
  auto reference_exists = reference_counter_->AddObjectLocation(object_id, node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object not found";
  }

  // For generator tasks where we haven't yet received the task reply, the
  // internal ObjectRefs may not be added yet, so we don't find out about these
  // until the task finishes.
  const auto &maybe_generator_id = task_manager_->TaskGeneratorId(object_id.TaskId());
  if (!maybe_generator_id.IsNil()) {
    if (task_manager_->ObjectRefStreamExists(maybe_generator_id)) {
      // ObjectRefStreamExists is used to distinguigsh num_returns="dynamic" vs
      // "streaming".
      task_manager_->TemporarilyOwnGeneratorReturnRefIfNeeded(object_id,
                                                              maybe_generator_id);
    } else {
      // The task is a generator and may not have finished yet. Add the internal
      // ObjectID so that we can update its location.
      reference_counter_->AddDynamicReturn(object_id, maybe_generator_id);
    }
    RAY_UNUSED(reference_counter_->AddObjectLocation(object_id, node_id));
  }
}

void CoreWorker::RemoveObjectLocationOwner(const ObjectID &object_id,
                                           const NodeID &node_id) {
  auto reference_exists = reference_counter_->RemoveObjectLocation(object_id, node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object not found";
  }
}

void CoreWorker::ProcessSubscribeObjectLocations(
    const rpc::WorkerObjectLocationsSubMessage &message) {
  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());
  const auto object_id = ObjectID::FromBinary(message.object_id());

  if (intended_worker_id != worker_context_->GetWorkerID()) {
    RAY_LOG(INFO) << "The ProcessSubscribeObjectLocations message is for worker "
                  << intended_worker_id << ", but the current worker is "
                  << worker_context_->GetWorkerID() << ". The RPC will be no-op.";
    object_info_publisher_->PublishFailure(
        rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL, object_id.Binary());
    return;
  }

  // Publish the first object location snapshot when subscribed for the first time.
  reference_counter_->PublishObjectLocationSnapshot(object_id);
}

std::unordered_map<rpc::LineageReconstructionTask, uint64_t>
CoreWorker::GetLocalOngoingLineageReconstructionTasks() const {
  return task_manager_->GetOngoingLineageReconstructionTasks(*actor_manager_);
}

Status CoreWorker::GetLocalObjectLocations(
    const std::vector<ObjectID> &object_ids,
    std::vector<std::optional<ObjectLocation>> *results) {
  results->clear();
  results->reserve(object_ids.size());
  if (object_ids.empty()) {
    return Status::OK();
  }
  for (size_t i = 0; i < object_ids.size(); i++) {
    results->emplace_back(TryGetLocalObjectLocation(*reference_counter_, object_ids[i]));
  }
  return Status::OK();
}

void CoreWorker::HandleGetObjectLocationsOwner(
    rpc::GetObjectLocationsOwnerRequest request,
    rpc::GetObjectLocationsOwnerReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  for (int i = 0; i < request.object_ids_size(); ++i) {
    auto object_id = ObjectID::FromBinary(request.object_ids(i));
    auto *object_info = reply->add_object_location_infos();
    reference_counter_->FillObjectInformation(object_id, object_info);
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::ProcessSubscribeForRefRemoved(
    const rpc::WorkerRefRemovedSubMessage &message) {
  const ObjectID &object_id = ObjectID::FromBinary(message.reference().object_id());

  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());
  if (intended_worker_id != worker_context_->GetWorkerID()) {
    RAY_LOG(INFO) << "The ProcessSubscribeForRefRemoved message is for worker "
                  << intended_worker_id << ", but the current worker is "
                  << worker_context_->GetWorkerID() << ". The RPC will be no-op.";
    reference_counter_->PublishRefRemoved(object_id);
    return;
  }

  const auto owner_address = message.reference().owner_address();
  ObjectID contained_in_id = ObjectID::FromBinary(message.contained_in_id());
  // So it will call PublishRefRemovedInternal to publish a message when the requested
  // object ID's ref count goes to 0.
  reference_counter_->SubscribeRefRemoved(object_id, contained_in_id, owner_address);
}

void CoreWorker::HandleRequestOwnerToCancelTask(
    rpc::RequestOwnerToCancelTaskRequest request,
    rpc::RequestOwnerToCancelTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = CancelTask(ObjectID::FromBinary(request.remote_object_id()),
                           request.force_kill(),
                           request.recursive());
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleCancelTask(rpc::CancelTaskRequest request,
                                  rpc::CancelTaskReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.intended_task_id());
  bool force_kill = request.force_kill();
  bool recursive = request.recursive();
  const auto &current_actor_id = worker_context_->GetCurrentActorID();
  const auto caller_worker_id = WorkerID::FromBinary(request.caller_worker_id());

  auto on_cancel_callback = [this,
                             reply,
                             send_reply_callback = std::move(send_reply_callback),
                             force_kill,
                             task_id](bool success, bool requested_task_running) {
    reply->set_attempt_succeeded(success);
    reply->set_requested_task_running(requested_task_running);
    send_reply_callback(Status::OK(), nullptr, nullptr);

    // Do force kill after reply callback sent.
    if (force_kill) {
      bool should_force_exit = false;
      std::string task_name;
      {
        // We grab the lock to make sure that we are force-killing the correct task.
        // We must release the lock before calling ForceExit, because ForceExit
        // also tries to acquire mutex_ to disconnect the services.
        absl::MutexLock lock(&mutex_);
        if (main_thread_task_id_ == task_id) {
          should_force_exit = true;
          task_name = main_thread_task_name_;
        }
      }
      if (should_force_exit) {
        ForceExit(rpc::WorkerExitType::INTENDED_USER_EXIT,
                  absl::StrCat("The worker exits because the task ",
                               task_name,
                               " has received a force ray.cancel request."));
      }
    }
  };

  if (task_id.ActorId() == current_actor_id) {
    RAY_LOG(INFO).WithField(task_id).WithField(current_actor_id)
        << "Cancel an actor task";
    CancelActorTaskOnExecutor(
        caller_worker_id, task_id, force_kill, recursive, std::move(on_cancel_callback));
  } else {
    RAY_CHECK(current_actor_id.IsNil());
    RAY_LOG(INFO).WithField(task_id) << "Cancel a normal task";
    CancelTaskOnExecutor(task_id, force_kill, recursive, on_cancel_callback);
  }
}

void CoreWorker::CancelTaskOnExecutor(TaskID task_id,
                                      bool force_kill,
                                      bool recursive,
                                      const OnCanceledCallback &on_canceled) {
  bool requested_task_running = false;
  {
    absl::MutexLock lock(&mutex_);
    requested_task_running = main_thread_task_id_ == task_id;

    if (requested_task_running) {
      canceled_tasks_.insert(task_id);
    }
  }
  bool success = requested_task_running;

  // Try non-force kill.
  // NOTE(swang): We do not hold the CoreWorker lock here because the kill
  // callback requires the GIL, which can cause a deadlock with the main task
  // thread. This means that the currently executing task can change by the time
  // the kill callback runs; the kill callback is responsible for also making
  // sure it cancels the right task.
  // See https://github.com/ray-project/ray/issues/29739.
  if (requested_task_running && !force_kill) {
    RAY_LOG(INFO).WithField(task_id) << "Cancelling a running task";
    success = options_.kill_main(task_id);
  } else if (!requested_task_running) {
    RAY_LOG(INFO).WithField(task_id)
        << "Cancelling a task that's not running. Tasks will be removed from a queue.";
    // If the task is not currently running, check if it is in the worker's queue of
    // normal tasks, and remove it if found.
    success = task_receiver_->CancelQueuedNormalTask(task_id);
  }
  if (recursive) {
    auto recursive_cancel = CancelChildren(task_id, force_kill);
    if (!recursive_cancel.ok()) {
      RAY_LOG(ERROR) << recursive_cancel.ToString();
    }
  }

  on_canceled(/*success=*/success, /*requested_task_running=*/requested_task_running);
}

void CoreWorker::CancelActorTaskOnExecutor(WorkerID caller_worker_id,
                                           TaskID task_id,
                                           bool force_kill,
                                           bool recursive,
                                           OnCanceledCallback on_canceled) {
  RAY_CHECK(!force_kill);
  auto is_async_actor = worker_context_->CurrentActorIsAsync();

  auto cancel = [this,
                 task_id,
                 caller_worker_id,
                 on_canceled = std::move(on_canceled),
                 is_async_actor]() {
    // If the task was still queued (not running yet), `CancelQueuedActorTask` will
    // cancel it. If it is already running, we attempt to cancel it.
    bool success = false;
    bool is_running = false;
    bool task_present = task_receiver_->CancelQueuedActorTask(caller_worker_id, task_id);
    if (task_present) {
      {
        absl::MutexLock lock(&mutex_);
        is_running = running_tasks_.find(task_id) != running_tasks_.end();

        if (is_running) {
          canceled_tasks_.insert(task_id);
        }
      }

      // Attempt to cancel the task if it's running.
      // We can't currently interrupt running tasks for non-async actors.
      if (is_running && is_async_actor) {
        success = options_.cancel_async_actor_task(task_id);
      } else {
        // If the task wasn't running, it was successfully cancelled by
        // CancelQueuedActorTask. Else if for non-async actor, we can't interrupt
        // running tasks, but we've marked it as canceled so IsTaskCanceled() will
        // return true. Return success so the client won't retry.
        success = true;
      }
    }

    on_canceled(success, is_running);
  };

  if (is_async_actor) {
    // If it is an async actor, post it to an execution service
    // to avoid thread issues. Note that when it is an async actor
    // task_execution_service_ won't actually run a task but it will
    // just create coroutines.
    task_execution_service_.post([cancel = std::move(cancel)]() { cancel(); },
                                 "CoreWorker.CancelActorTaskOnExecutor");
  } else {
    // For regular actor, we cannot post it to task_execution_service because
    // main thread is blocked. Threaded actor can do both (dispatching to
    // task execution service, or just directly call it in io_service).
    // There's no special reason why we don't dispatch
    // cancel to task_execution_service_ for threaded actors.
    cancel();
  }

  if (recursive) {
    auto recursive_cancel = CancelChildren(task_id, force_kill);
    if (!recursive_cancel.ok()) {
      RAY_LOG(ERROR) << recursive_cancel.ToString();
    }
  };
}

void CoreWorker::HandleKillActor(rpc::KillActorRequest request,
                                 rpc::KillActorReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  ActorID intended_actor_id = ActorID::FromBinary(request.intended_actor_id());
  if (intended_actor_id != worker_context_->GetCurrentActorID()) {
    std::ostringstream stream;
    stream << "Mismatched ActorID: ignoring KillActor for previous actor "
           << intended_actor_id
           << ", current actor ID: " << worker_context_->GetCurrentActorID();
    const auto &msg = stream.str();
    RAY_LOG(ERROR) << msg;
    send_reply_callback(Status::Invalid(msg), nullptr, nullptr);
    return;
  }

  const auto &kill_actor_reason =
      gcs::GenErrorMessageFromDeathCause(request.death_cause());

  if (request.force_kill()) {
    RAY_LOG(INFO) << "Force kill actor request has received. exiting immediately... "
                  << kill_actor_reason;
    RAY_LOG(DEBUG) << "HandleKillActor: About to call ForceExit";
    // If we don't need to restart this actor, we notify raylet before force killing it.
    ForceExit(
        rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
        absl::StrCat("Worker exits because the actor is killed. ", kill_actor_reason));
    RAY_LOG(DEBUG) << "HandleKillActor: ForceExit completed";
  } else {
    RAY_LOG(DEBUG) << "HandleKillActor: About to call Exit";
    Exit(rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
         absl::StrCat("Worker exits because the actor is killed. ", kill_actor_reason));
  }
}

void CoreWorker::HandleRegisterMutableObjectReader(
    rpc::RegisterMutableObjectReaderRequest request,
    rpc::RegisterMutableObjectReaderReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  local_raylet_rpc_client_->RegisterMutableObjectReader(
      ObjectID::FromBinary(request.writer_object_id()),
      request.num_readers(),
      ObjectID::FromBinary(request.reader_object_id()),
      [send_reply_callback](const Status &status,
                            const rpc::RegisterMutableObjectReply &r) {
        RAY_CHECK_OK(status);
        send_reply_callback(Status::OK(), nullptr, nullptr);
      });
}

int64_t CoreWorker::GetLocalMemoryStoreBytesUsed() const {
  MemoryStoreStats memory_store_stats = memory_store_->GetMemoryStoreStatisticalData();
  return memory_store_stats.num_local_objects_bytes;
}

void CoreWorker::HandleGetCoreWorkerStats(rpc::GetCoreWorkerStatsRequest request,
                                          rpc::GetCoreWorkerStatsReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  auto limit = request.has_limit() ? request.limit() : -1;
  auto stats = reply->mutable_core_worker_stats();
  // TODO(swang): Differentiate between tasks that are currently pending
  // execution and tasks that have finished but may be retried.
  stats->set_num_pending_tasks(task_manager_->NumSubmissibleTasks());
  stats->set_task_queue_length(task_queue_length_);
  stats->set_num_executed_tasks(num_executed_tasks_);
  stats->set_num_object_refs_in_scope(reference_counter_->NumObjectIDsInScope());
  stats->set_num_owned_objects(reference_counter_->NumObjectsOwnedByUs());
  stats->set_num_owned_actors(reference_counter_->NumActorsOwnedByUs());
  stats->set_ip_address(rpc_address_.ip_address());
  stats->set_port(rpc_address_.port());
  stats->set_pid(getpid());
  stats->set_language(options_.language);
  stats->set_job_id(worker_context_->GetCurrentJobID().Binary());
  stats->set_worker_id(worker_context_->GetWorkerID().Binary());
  stats->set_actor_id(actor_id_.Binary());
  stats->set_worker_type(worker_context_->GetWorkerType());
  stats->set_num_running_tasks(running_tasks_.size());
  stats->set_num_in_flight_arg_pinning_requests(num_get_pin_args_in_flight_);
  stats->set_num_of_failed_arg_pinning_requests(num_failed_get_pin_args_);
  auto *used_resources_map = stats->mutable_used_resources();
  for (auto const &[resource_name, resource_allocations] : resource_ids_) {
    rpc::ResourceAllocations allocations;
    for (auto const &[cur_resource_slot, cur_resource_alloc] : resource_allocations) {
      auto resource_slot = allocations.add_resource_slots();
      resource_slot->set_slot(cur_resource_slot);
      resource_slot->set_allocation(cur_resource_alloc);
    }
    (*used_resources_map)[resource_name] = allocations;
  }
  MemoryStoreStats memory_store_stats = memory_store_->GetMemoryStoreStatisticalData();
  stats->set_num_in_plasma(memory_store_stats.num_in_plasma);
  stats->set_num_local_objects(memory_store_stats.num_local_objects);
  stats->set_used_object_store_memory(memory_store_stats.num_local_objects_bytes);

  if (request.include_memory_info()) {
    reference_counter_->AddObjectRefStats(
        plasma_store_provider_->UsedObjectsList(), stats, limit);
    task_manager_->AddTaskStatusInfo(stats);
  }

  if (request.include_task_info()) {
    task_manager_->FillTaskInfo(reply, limit);
    for (const auto &current_running_task : running_tasks_) {
      reply->add_running_task_ids(current_running_task.second.TaskIdBinary());
    }
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleLocalGC(rpc::LocalGCRequest request,
                               rpc::LocalGCReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  if (options_.gc_collect != nullptr) {
    options_.gc_collect();
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("GC callback not defined"), nullptr, nullptr);
  }
}

void CoreWorker::HandleDeleteObjects(rpc::DeleteObjectsRequest request,
                                     rpc::DeleteObjectsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  for (const auto &obj_id : request.object_ids()) {
    object_ids.push_back(ObjectID::FromBinary(obj_id));
  }
  auto status = DeleteImpl(object_ids, request.local_only());
  send_reply_callback(status, nullptr, nullptr);
}

Status CoreWorker::DeleteImpl(const std::vector<ObjectID> &object_ids, bool local_only) {
  // Release the object from plasma. This does not affect the object's ref
  // count. If this was called from a non-owning worker, then a warning will be
  // logged and the object will not get released.
  reference_counter_->FreePlasmaObjects(object_ids);

  // Store an error in the in-memory store to indicate that the plasma value is
  // no longer reachable.
  memory_store_->Delete(object_ids);
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG).WithField(object_id) << "Freeing object";
    memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_FREED),
                       object_id,
                       reference_counter_->HasReference(object_id));
  }

  // We only delete from plasma, which avoids hangs (issue #7105). In-memory
  // objects can only be deleted once the ref count goes to 0.
  absl::flat_hash_set<ObjectID> plasma_object_ids(object_ids.begin(), object_ids.end());
  return plasma_store_provider_->Delete(plasma_object_ids, local_only);
}

void CoreWorker::HandleSpillObjects(rpc::SpillObjectsRequest request,
                                    rpc::SpillObjectsReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  if (options_.spill_objects != nullptr) {
    auto object_refs = VectorFromProtobuf<rpc::ObjectReference>(
        std::move(*request.mutable_object_refs_to_spill()));
    std::vector<std::string> object_urls = options_.spill_objects(object_refs);
    for (size_t i = 0; i < object_urls.size(); i++) {
      reply->add_spilled_objects_url(std::move(object_urls[i]));
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Spill objects callback not defined"), nullptr, nullptr);
  }
}

void CoreWorker::HandleRestoreSpilledObjects(rpc::RestoreSpilledObjectsRequest request,
                                             rpc::RestoreSpilledObjectsReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  if (options_.restore_spilled_objects != nullptr) {
    // Get a list of object ids.
    std::vector<rpc::ObjectReference> object_refs_to_restore;
    object_refs_to_restore.reserve(request.object_ids_to_restore_size());
    for (const auto &id_binary : request.object_ids_to_restore()) {
      rpc::ObjectReference ref;
      ref.set_object_id(id_binary);
      object_refs_to_restore.push_back(std::move(ref));
    }
    // Get a list of spilled_object_urls.
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    auto total =
        options_.restore_spilled_objects(object_refs_to_restore, spilled_objects_url);
    reply->set_bytes_restored_total(total);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Restore spilled objects callback not defined"),
        nullptr,
        nullptr);
  }
}

void CoreWorker::HandleDeleteSpilledObjects(rpc::DeleteSpilledObjectsRequest request,
                                            rpc::DeleteSpilledObjectsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  if (options_.delete_spilled_objects != nullptr) {
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    options_.delete_spilled_objects(spilled_objects_url,
                                    worker_context_->GetWorkerType());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Delete spilled objects callback not defined"),
        nullptr,
        nullptr);
  }
}

void CoreWorker::HandleExit(rpc::ExitRequest request,
                            rpc::ExitReply *reply,
                            rpc::SendReplyCallback send_reply_callback) {
  bool is_idle = IsIdle();
  bool force_exit = request.force_exit();
  RAY_LOG(DEBUG) << "Exiting: is_idle: " << is_idle << " force_exit: " << force_exit;
  if (!is_idle) {
    const size_t num_pending_tasks = task_manager_->NumPendingTasks();
    const int64_t pins_in_flight = local_raylet_rpc_client_->GetPinsInFlight();
    RAY_LOG_EVERY_MS(INFO, 60000)
        << "Worker is not idle: reference counter: " << reference_counter_->DebugString()
        << " # pins in flight: " << pins_in_flight
        << " # pending tasks: " << num_pending_tasks;
    if (force_exit) {
      RAY_LOG(INFO) << "Force exiting worker that's not idle. "
                    << "reference counter: " << reference_counter_->DebugString()
                    << " # Pins in flight: " << pins_in_flight
                    << " # pending tasks: " << num_pending_tasks;
    }
  }
  const bool will_exit = is_idle || force_exit;
  reply->set_success(will_exit);
  send_reply_callback(
      Status::OK(),
      [this, will_exit, force_exit]() {
        if (!will_exit) {
          return;
        }

        ShutdownReason reason;
        std::string detail;

        if (force_exit) {
          reason = ShutdownReason::kForcedExit;
          detail = "Worker force exited because its job has finished";
        } else {
          reason = ShutdownReason::kIdleTimeout;
          detail = "Worker exited because it was idle for a long time";
        }

        shutdown_coordinator_->RequestShutdown(force_exit, reason, detail);
      },
      // Fallback on RPC failure - still attempt shutdown
      [this]() {
        shutdown_coordinator_->RequestShutdown(
            /*force_shutdown=*/false,
            ShutdownReason::kIdleTimeout,
            "Worker exited due to RPC failure during idle exit");
      });
}

// Handle RPC for TaskManager::NumPendingTasks().
void CoreWorker::HandleNumPendingTasks(rpc::NumPendingTasksRequest request,
                                       rpc::NumPendingTasksReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Received NumPendingTasks request.";
  reply->set_num_pending_tasks(task_manager_->NumPendingTasks());
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::YieldCurrentFiber(FiberEvent &event) {
  RAY_CHECK(worker_context_->CurrentActorIsAsync());
  boost::this_fiber::yield();
  event.Wait();
}

void CoreWorker::GetAsync(const ObjectID &object_id,
                          SetResultCallback success_callback,
                          void *python_user_callback) {
  auto fallback_callback = std::bind(&CoreWorker::PlasmaCallback,
                                     this,
                                     success_callback,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3);

  memory_store_->GetAsync(
      object_id,
      // This callback is posted to io_service_ by memory store.
      [object_id,
       python_user_callback,
       success_callback = std::move(success_callback),
       fallback_callback =
           std::move(fallback_callback)](std::shared_ptr<RayObject> ray_object) {
        if (ray_object->IsInPlasmaError()) {
          fallback_callback(ray_object, object_id, python_user_callback);
        } else {
          success_callback(ray_object, object_id, python_user_callback);
        }
      });
}

void CoreWorker::PlasmaCallback(const SetResultCallback &success,
                                const std::shared_ptr<RayObject> &ray_object,
                                ObjectID object_id,
                                void *py_future) {
  RAY_CHECK(ray_object->IsInPlasmaError());

  // First check if the object is available in local plasma store.
  // Note that we are using Contains instead of Get so it won't trigger pull request
  // to remote nodes.
  bool object_is_local = false;
  if (Contains(object_id, &object_is_local).ok() && object_is_local) {
    std::vector<std::shared_ptr<RayObject>> vec;
    if (Get(std::vector<ObjectID>{object_id}, 0, vec).ok()) {
      RAY_CHECK(!vec.empty())
          << "Failed to get local object but Raylet notified object is local.";
      return success(vec.front(), object_id, py_future);
    }
  }

  // Object is not available locally. We now add the callback to listener queue.
  {
    absl::MutexLock lock(&plasma_mutex_);
    auto plasma_arrived_callback = [this, success, object_id, py_future]() {
      // This callback is invoked on the io_service_ event loop, so it cannot call
      // blocking call like Get(). We used GetAsync here, which should immediate call
      // PlasmaCallback again with object available locally.
      GetAsync(object_id, success, py_future);
    };

    async_plasma_callbacks_[object_id].emplace_back(std::move(plasma_arrived_callback));
  }

  // Ask raylet to subscribe to object notification. Raylet will call this core worker
  // when the object is local (and it will fire the callback immediately if the object
  // exists). CoreWorker::HandlePlasmaObjectReady handles such request.
  auto owner_address = GetOwnerAddressOrDie(object_id);
  raylet_ipc_client_->SubscribePlasmaReady(object_id, owner_address);
}

void CoreWorker::HandlePlasmaObjectReady(rpc::PlasmaObjectReadyRequest request,
                                         rpc::PlasmaObjectReadyReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::function<void(void)>> callbacks;
  {
    absl::MutexLock lock(&plasma_mutex_);
    auto it = async_plasma_callbacks_.extract(ObjectID::FromBinary(request.object_id()));
    callbacks = it.mapped();
  }
  for (const auto &callback : callbacks) {
    // This callback needs to be asynchronous because it runs on the io_service_, so no
    // RPCs can be processed while it's running. This can easily lead to deadlock (for
    // example if the callback calls ray.get() on an object that is dependent on an RPC
    // to be ready).
    callback();
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::SetActorId(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(actor_id_.IsNil());
  actor_id_ = actor_id;
}

rpc::JobConfig CoreWorker::GetJobConfig() const {
  return worker_context_->GetCurrentJobConfig();
}

bool CoreWorker::IsExiting() const { return shutdown_coordinator_->ShouldEarlyExit(); }

bool CoreWorker::IsIdle(size_t num_objects_with_references,
                        int64_t pins_in_flight,
                        size_t num_pending_tasks) const {
  return (num_objects_with_references == 0) && (pins_in_flight == 0) &&
         (num_pending_tasks == 0);
}

bool CoreWorker::IsIdle() const {
  const size_t num_objects_with_references = reference_counter_->Size();
  const size_t num_pending_tasks = task_manager_->NumPendingTasks();
  const int64_t pins_in_flight = local_raylet_rpc_client_->GetPinsInFlight();
  return IsIdle(num_objects_with_references, pins_in_flight, num_pending_tasks);
}

Status CoreWorker::WaitForActorRegistered(const std::vector<ObjectID> &ids) {
  std::vector<ActorID> actor_ids;
  for (const auto &id : ids) {
    if (ObjectID::IsActorID(id)) {
      actor_ids.emplace_back(ObjectID::ToActorID(id));
    }
  }
  if (actor_ids.empty()) {
    return Status::OK();
  }
  std::promise<void> promise;
  auto future = promise.get_future();
  std::vector<Status> ret;
  int counter = 0;
  // Post to service pool to avoid mutex
  io_service_.post(
      [&, this]() {
        for (const auto &id : actor_ids) {
          if (actor_creator_->IsActorInRegistering(id)) {
            ++counter;
            actor_creator_->AsyncWaitForActorRegisterFinish(
                id, [&counter, &promise, &ret](const Status &status) {
                  ret.push_back(status);
                  --counter;
                  if (counter == 0) {
                    promise.set_value();
                  }
                });
          }
        }
        if (counter == 0) {
          promise.set_value();
        }
      },
      "CoreWorker.WaitForActorRegistered");
  future.wait();
  for (const auto &s : ret) {
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

std::vector<ObjectID> CoreWorker::GetCurrentReturnIds(int num_returns,
                                                      const ActorID &callee_actor_id) {
  std::vector<ObjectID> return_ids(num_returns);
  const auto next_task_index = worker_context_->GetTaskIndex() + 1;
  TaskID task_id;
  if (callee_actor_id.IsNil()) {
    /// Return ids for normal task call.
    task_id = TaskID::ForNormalTask(worker_context_->GetCurrentJobID(),
                                    worker_context_->GetCurrentInternalTaskId(),
                                    next_task_index);
  } else {
    /// Return ids for actor task call.
    task_id = TaskID::ForActorTask(worker_context_->GetCurrentJobID(),
                                   worker_context_->GetCurrentInternalTaskId(),
                                   next_task_index,
                                   callee_actor_id);
  }
  for (int i = 0; i < num_returns; i++) {
    return_ids[i] = ObjectID::FromIndex(task_id, i + 1);
  }
  return return_ids;
}

void CoreWorker::RecordTaskLogStart(const TaskID &task_id,
                                    int32_t attempt_number,
                                    const std::string &stdout_path,
                                    const std::string &stderr_path,
                                    int64_t stdout_start_offset,
                                    int64_t stderr_start_offset) const {
  rpc::TaskLogInfo task_log_info;
  task_log_info.set_stdout_file(stdout_path);
  task_log_info.set_stderr_file(stderr_path);
  task_log_info.set_stdout_start(stdout_start_offset);
  task_log_info.set_stderr_start(stderr_start_offset);

  auto current_task = worker_context_->GetCurrentTask();
  RAY_CHECK(current_task)
      << "We should have set the current task spec while executing the task.";
  RAY_UNUSED(task_event_buffer_->RecordTaskStatusEventIfNeeded(
      task_id,
      worker_context_->GetCurrentJobID(),
      attempt_number,
      *current_task,
      rpc::TaskStatus::NIL,
      /*include_task_info=*/false,
      worker::TaskStatusEvent::TaskStateUpdate(task_log_info)));
}

void CoreWorker::RecordTaskLogEnd(const TaskID &task_id,
                                  int32_t attempt_number,
                                  int64_t stdout_end_offset,
                                  int64_t stderr_end_offset) const {
  rpc::TaskLogInfo task_log_info;
  task_log_info.set_stdout_end(stdout_end_offset);
  task_log_info.set_stderr_end(stderr_end_offset);

  auto current_task = worker_context_->GetCurrentTask();
  RAY_CHECK(current_task)
      << "We should have set the current task spec before executing the task.";
  RAY_UNUSED(task_event_buffer_->RecordTaskStatusEventIfNeeded(
      task_id,
      worker_context_->GetCurrentJobID(),
      attempt_number,
      *current_task,
      rpc::TaskStatus::NIL,
      /*include_task_info=*/false,
      worker::TaskStatusEvent::TaskStateUpdate(task_log_info)));
}

void CoreWorker::UpdateTaskIsDebuggerPaused(const TaskID &task_id,
                                            const bool is_debugger_paused) {
  absl::MutexLock lock(&mutex_);
  auto running_task_it = running_tasks_.find(task_id);
  RAY_CHECK(running_task_it != running_tasks_.end())
      << "We should have set the running task spec before running the task.";
  RAY_LOG(DEBUG).WithField(running_task_it->second.TaskId())
      << "Task is paused by debugger set to " << is_debugger_paused;
  RAY_UNUSED(task_event_buffer_->RecordTaskStatusEventIfNeeded(
      task_id,
      worker_context_->GetCurrentJobID(),
      running_task_it->second.AttemptNumber(),
      running_task_it->second,
      rpc::TaskStatus::NIL,
      /*include_task_info=*/false,
      worker::TaskStatusEvent::TaskStateUpdate(is_debugger_paused)));
}

void CoreWorker::AsyncRetryTask(TaskSpecification &spec, uint32_t delay_ms) {
  spec.GetMutableMessage().set_attempt_number(spec.AttemptNumber() + 1);
  absl::MutexLock lock(&mutex_);
  TaskToRetry task_to_retry{clock_.SteadyNowMillis() + delay_ms, spec};
  RAY_LOG(INFO) << "Will resubmit task after a " << delay_ms
                << "ms delay: " << spec.DebugString();
  to_resubmit_.push(std::move(task_to_retry));
}

std::shared_ptr<RayletClientInterface> CoreWorker::GetRayletRpcClient(
    const NodeID &node_id) {
  if (node_id == GetCurrentNodeId()) {
    return local_raylet_rpc_client_;
  }
  auto node_info =
      gcs_client_->Nodes().GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/true);
  if (!node_info) {
    return nullptr;
  }
  auto address = rpc::RayletClientPool::GenerateRayletAddress(
      node_id, node_info->node_manager_address(), node_info->node_manager_port());
  return raylet_client_pool_->GetOrConnectByAddress(address);
}

void CoreWorker::FreeObjectOnNodesAsync(const ObjectID &object_id,
                                        const absl::flat_hash_set<NodeID> &locations) {
  rpc::FreeLocalObjectsRequest request;
  request.add_object_ids(object_id.Binary());

  for (const auto &node_id : locations) {
    auto client = GetRayletRpcClient(node_id);
    if (client == nullptr) {
      continue;
    }
    client->FreeLocalObjects(request);
  }
}

std::vector<rpc::Address> CoreWorker::SelectRecoveryWitnesses(
    const TaskID &task_id) const {
  const uint32_t requested_count =
      recovery_witness_holder_baseline_enabled_
          ? RayConfig::instance()
                .recovery_succession_target_holder_count()
          : RayConfig::instance()
                .recovery_succession_witness_count();

  if (!recovery_succession_enabled_ ||
      requested_count == 0 ||
      recovery_witness_node_cache_ == nullptr) {
    return {};
  }

  // Snapshot the current ALIVE witness nodes from the cache.
  // Do not hold the cache mutex while hashing/sorting candidates.
  std::vector<std::pair<NodeID, rpc::Address>> alive_witnesses;

  {
    std::unique_lock<std::mutex> lock(
        recovery_witness_node_cache_->mutex);

    // The GCS node subscription provides the initial node snapshot and then
    // keeps this cache updated through ALIVE/DEAD notifications.
    //
    // Keep the same bounded behavior as the old GetAllNoCache(timeout=5000)
    // path instead of potentially blocking task submission forever.
    const bool initialized =
        recovery_witness_node_cache_->cv.wait_for(
            lock,
            std::chrono::milliseconds(5000),
            [this]() {
              return recovery_witness_node_cache_->initialized;
            });

    if (!initialized) {
      RAY_LOG(WARNING).WithField(task_id)
          << "Timed out waiting for recovery witness node cache "
             "initialization.";
      return {};
    }

    if (!recovery_witness_node_cache_->subscription_ok) {
      RAY_LOG(WARNING).WithField(task_id)
          << "Recovery witness node cache is unavailable because "
             "the GCS node subscription failed.";
      return {};
    }

    alive_witnesses.reserve(
        recovery_witness_node_cache_->alive_nodes.size());

    for (const auto &[node_id, address] :
         recovery_witness_node_cache_->alive_nodes) {
      alive_witnesses.emplace_back(
          node_id,
          address);
    }
  }

  struct WitnessCandidate {
    uint64_t score;
    rpc::Address address;
  };

  std::vector<WitnessCandidate> candidates;
  candidates.reserve(alive_witnesses.size());

  // Preserve the exact deterministic per-task witness scores.
  const bool optimized_baseline_selection =
      recovery_witness_holder_baseline_enabled_;
  const std::string task_id_binary =
      optimized_baseline_selection ? task_id.Binary() : std::string();

  for (auto &[node_id, address] : alive_witnesses) {
    candidates.push_back(
        WitnessCandidate{
            optimized_baseline_selection
                ? StableWitnessScoreOptimized(task_id_binary, node_id)
                : StableWitnessScore(task_id, node_id),
            std::move(address)});
  }

  const size_t selected_count =
      std::min<size_t>(
          requested_count,
          candidates.size());

  const auto better_witness =
      [](const WitnessCandidate &left,
         const WitnessCandidate &right) {
        if (left.score != right.score) {
          return left.score > right.score;
        }
        return left.address.node_id() < right.address.node_id();
      };

  if (recovery_witness_holder_baseline_enabled_ &&
      selected_count < candidates.size()) {
    // O(N) partition plus O(R log R) ordering instead of O(N log N).
    std::nth_element(
        candidates.begin(),
        candidates.begin() + selected_count,
        candidates.end(),
        better_witness);
    std::sort(
        candidates.begin(),
        candidates.begin() + selected_count,
        better_witness);
  } else {
    std::sort(candidates.begin(), candidates.end(), better_witness);
  }

  std::vector<rpc::Address> witnesses;
  witnesses.reserve(selected_count);

  for (size_t index = 0;
       index < selected_count;
       ++index) {
    witnesses.push_back(
        std::move(candidates[index].address));
  }

  return witnesses;
}




void CoreWorker::PopulateRecoveryWitnesses(rpc::RecoveryManifest *manifest) const {
  if (manifest == nullptr || manifest->task_id().empty()) {
    return;
  }

  manifest->clear_witness_raylets();

  const TaskID task_id = TaskID::FromBinary(manifest->task_id());

  std::vector<rpc::Address> witnesses = SelectRecoveryWitnesses(task_id);

  for (const rpc::Address &witness : witnesses) {
    manifest->add_witness_raylets()->CopyFrom(witness);
  }

  manifest->set_witness_count(static_cast<uint32_t>(witnesses.size()));
}

void CoreWorker::PublishRecoveryManifestToWitnesses(
    const rpc::RecoveryManifest &manifest,
    RecoveryWitnessPublishCallback callback,
    const rpc::TaskSpec *task_spec,
    const std::string *serialized_task_spec) const {

  RAY_CHECK(task_spec == nullptr || serialized_task_spec == nullptr);
  const bool require_all_witnesses =
      task_spec != nullptr || serialized_task_spec != nullptr;

  if (!recovery_succession_enabled_ || manifest.task_id().empty() ||
      manifest.witness_raylets_size() == 0) {
    callback(false, std::nullopt);
    return;
  }

  struct PublishState {
    absl::Mutex mutex;

    size_t completed ABSL_GUARDED_BY(mutex) = 0;
    size_t stored_count ABSL_GUARDED_BY(mutex) = 0;

    bool callback_sent ABSL_GUARDED_BY(mutex) = false;

    std::optional<rpc::RecoveryManifest>
        newest_manifest ABSL_GUARDED_BY(mutex);
  };

  const size_t witness_count = static_cast<size_t>(manifest.witness_raylets_size());

  // Certificates use their separate publisher. Keep tombstones and any
  // certificate-mode ordinary publications on the original callback path too.
  const bool batch_ack = recovery_witness_ack_batch_handler_ != nullptr &&
      !manifest.tombstoned() &&
      !RayConfig::instance().enable_recovery_succession_certificate_admission();
  std::shared_ptr<BatchedWitnessPublication> batch_publication;
  std::shared_ptr<PublishState> state;
  if (batch_ack) {
    batch_publication = std::make_shared<BatchedWitnessPublication>();
    batch_publication->witness_count = witness_count;
    batch_publication->require_all_witnesses = require_all_witnesses;
    batch_publication->callback = callback;
  } else {
    state = std::make_shared<PublishState>();
  }

  for (const rpc::Address &witness : manifest.witness_raylets()) {
    const uint64_t witness_request_build_start_ns =
        recovery_succession_profiling_enabled_ && !manifest.tombstoned()
            ? RecoveryProfileNowNs()
            : 0;
    rpc::UpdateRecoveryWitnessRequest request;
    request.mutable_manifest()->CopyFrom(manifest);

    if (serialized_task_spec != nullptr) {
      request.set_serialized_task_spec(*serialized_task_spec);
    } else if (task_spec != nullptr) {
      request.mutable_task_spec()->CopyFrom(*task_spec);

      if (recovery_witness_holder_baseline_enabled_) {
        // Even with separate retained storage, installation uses the original
        // full-lineage baseline wire contract. The manifest is removed only
        // after the witness has validated and accepted this request.
        request.mutable_task_spec()
            ->mutable_recovery_manifest()
            ->CopyFrom(manifest);
      }
    }

    auto witness_client = raylet_client_pool_->GetOrConnectByAddress(witness);
    if (witness_request_build_start_ns != 0) {
      recovery_succession_manager_->RecordWitnessRequestBuildCpu(
          RecoveryProfileNowNs() - witness_request_build_start_ns);
    }

    uint64_t witness_start_ns = 0;

    if (recovery_succession_profiling_enabled_ &&
        !manifest.tombstoned()) {
      const uint64_t task_spec_bytes =
          !request.serialized_task_spec().empty()
              ? static_cast<uint64_t>(request.serialized_task_spec().size())
              : (request.has_task_spec()
                     ? static_cast<uint64_t>(request.task_spec().ByteSizeLong())
                     : 0);

      recovery_succession_manager_
          ->RecordWitnessUpdateRpcSent(
              task_spec_bytes,
              static_cast<uint64_t>(
                  manifest.ByteSizeLong()));

      witness_start_ns = RecoveryProfileNowNs();
    }

    if (batch_ack) {
      auto context = std::make_shared<BatchedWitnessAckContext>();
      context->publication = batch_publication;
      context->witness_start_ns = witness_start_ns;
      witness_client->UpdateRecoveryWitnessWithBatchHandler(
          std::move(request), std::move(context), recovery_witness_ack_batch_handler_);
      continue;
    }

    witness_client->UpdateRecoveryWitness(
    std::move(request),
    [state,
      witness_count,
      require_all_witnesses,
      callback,
      manager = recovery_succession_manager_,
      witness_start_ns](
        const Status &status,
        rpc::UpdateRecoveryWitnessReply &&reply) mutable {


      if (witness_start_ns != 0) {
        manager->RecordWitnessUpdateRpcLatency(
            RecoveryProfileNowNs() - witness_start_ns);
        manager->RecordWitnessUpdateRpcBreakdown(
            reply.client_queue_time_ns(),
            reply.client_submit_to_cq_time_ns(),
            reply.client_cq_to_main_loop_time_ns(),
            reply.client_main_loop_to_batch_callback_time_ns(),
            reply.client_enqueue_cpu_time_ns(),
            reply.client_batch_build_cpu_time_ns(),
            reply.client_batch_demux_cpu_time_ns(),
            reply.witness_batch_queue_time_ns(),
            reply.witness_handler_time_ns(),
            reply.witness_mutex_wait_time_ns(),
            reply.witness_mutex_hold_time_ns(),
            reply.client_batch_leader(),
            reply.client_batch_size());
      }

      const uint64_t witness_callback_cpu_start_ns =
          witness_start_ns != 0 ? RecoveryProfileNowNs() : 0;
      bool report_success = false;
      bool report_failure = false;

      std::optional<rpc::RecoveryManifest> newest_manifest;

      {
        absl::MutexLock lock(&state->mutex);

        ++state->completed;

        const bool this_witness_stored =
            status.ok() && reply.stored();

        if (this_witness_stored) {
          ++state->stored_count;
        }

        if (reply.has_latest_manifest()) {
          if (!state->newest_manifest.has_value() ||
              CompareRecoveryManifestVersions(
                  reply.latest_manifest(),
                  state->newest_manifest.value()) > 0) {
            state->newest_manifest =
                reply.latest_manifest();
          }
        }

        if (!state->callback_sent) {
          if (require_all_witnesses) {
            // Witness-as-holder baseline:
            // wait until every selected witness has replied.
            if (state->completed == witness_count) {
              state->callback_sent = true;

              if (state->stored_count == witness_count) {
                report_success = true;
              } else {
                newest_manifest = state->newest_manifest;
                report_failure = true;
              }
            }
          } else {
            // Existing recovery succession:
            // one compact-witness acknowledgement is sufficient.
            if (this_witness_stored) {
              state->callback_sent = true;
              report_success = true;
            } else if (state->completed == witness_count) {
              state->callback_sent = true;
              newest_manifest = state->newest_manifest;
              report_failure = true;
            }
          }
        }
      }

      if (report_success) {
        callback(true, std::nullopt);
      } else if (report_failure) {
        callback(false, std::move(newest_manifest));
      }
      if (witness_callback_cpu_start_ns != 0) {
        manager->RecordWitnessLogicalCallbackCpu(
            RecoveryProfileNowNs() - witness_callback_cpu_start_ns,
            report_success || report_failure);
      }
    });



  }
}

void CoreWorker::PublishRecoveryHolderCertificateToWitnesses(
    const rpc::RecoveryManifest &manifest,
    const rpc::RecoveryHolderCertificate &certificate,
    RecoveryWitnessPublishCallback callback) const {
  if (!recovery_succession_enabled_ || manifest.task_id().empty() ||
      manifest.witness_raylets_size() == 0 || certificate.task_id() != manifest.task_id()) {
    callback(false, std::nullopt);
    return;
  }

  struct PublishState {
    absl::Mutex mutex;
    size_t completed ABSL_GUARDED_BY(mutex) = 0;
    bool callback_sent ABSL_GUARDED_BY(mutex) = false;
    std::optional<rpc::RecoveryManifest> newest ABSL_GUARDED_BY(mutex);
  };

  auto state = std::make_shared<PublishState>();
  const size_t witness_count = static_cast<size_t>(manifest.witness_raylets_size());

  for (const rpc::Address &witness : manifest.witness_raylets()) {
    rpc::UpdateRecoveryWitnessRequest request;
    // Carry the last committed/base view only as a bootstrap in case this
    // witness missed lazy activation.  An existing witness never replaces its
    // merged set with this base; it only unions the certificate.
    request.mutable_manifest()->CopyFrom(manifest);
    request.mutable_holder_certificate()->CopyFrom(certificate);

    const uint64_t witness_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
    if (witness_start_ns != 0) {
      recovery_succession_manager_->RecordWitnessUpdateRpcSent(
          0, static_cast<uint64_t>(certificate.ByteSizeLong()));
    }

    auto witness_client = raylet_client_pool_->GetOrConnectByAddress(witness);
    witness_client->UpdateRecoveryWitness(
        std::move(request),
        [state,
         witness_count,
         callback,
         manager = recovery_succession_manager_,
         witness_start_ns](const Status &status,
                           rpc::UpdateRecoveryWitnessReply &&reply) mutable {
          if (witness_start_ns != 0) {
            manager->RecordWitnessUpdateRpcLatency(
                RecoveryProfileNowNs() - witness_start_ns);
            manager->RecordWitnessUpdateRpcBreakdown(
            reply.client_queue_time_ns(),
            reply.client_submit_to_cq_time_ns(),
            reply.client_cq_to_main_loop_time_ns(),
            reply.client_main_loop_to_batch_callback_time_ns(),
            reply.client_enqueue_cpu_time_ns(),
            reply.client_batch_build_cpu_time_ns(),
            reply.client_batch_demux_cpu_time_ns(),
            reply.witness_batch_queue_time_ns(),
            reply.witness_handler_time_ns(),
            reply.witness_mutex_wait_time_ns(),
            reply.witness_mutex_hold_time_ns(),
            reply.client_batch_leader(),
            reply.client_batch_size());
          }

          bool success = false;
          bool failure = false;
          std::optional<rpc::RecoveryManifest> newest;
          {
            absl::MutexLock lock(&state->mutex);
            ++state->completed;

            if (reply.has_latest_manifest()) {
              if (!state->newest.has_value() ||
                  CompareRecoveryManifestVersions(reply.latest_manifest(),
                                                  state->newest.value()) > 0) {
                state->newest = reply.latest_manifest();
              }
            }

            if (!state->callback_sent) {
              if (status.ok() && reply.stored()) {
                // Preserve current Succession durability semantics: one compact
                // witness acknowledgement is sufficient.
                state->callback_sent = true;
                success = true;
              } else if (state->completed == witness_count) {
                state->callback_sent = true;
                newest = state->newest;
                failure = true;
              }
            }
          }

          if (success) {
            callback(true, std::nullopt);
          } else if (failure) {
            callback(false, std::move(newest));
          }
        });
  }
}


void CoreWorker::LookupRecoveryManifestFromWitnesses(
    const rpc::RecoveryManifest &cached_manifest,
    RecoveryWitnessLookupCallback callback) {
  if (!recovery_succession_enabled_ || cached_manifest.task_id().empty() ||
      cached_manifest.witness_raylets_size() == 0) {
    callback(std::nullopt);
    return;
  }

  struct LookupState {
    absl::Mutex mutex;
    size_t completed ABSL_GUARDED_BY(mutex) = 0;
    std::optional<rpc::RecoveryManifest> merged ABSL_GUARDED_BY(mutex);
  };

  auto state = std::make_shared<LookupState>();
  const size_t witness_count =
      static_cast<size_t>(cached_manifest.witness_raylets_size());
  const bool certificate_mode =
      RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !recovery_witness_holder_baseline_enabled_;

  for (const rpc::Address &witness : cached_manifest.witness_raylets()) {
    rpc::GetRecoveryWitnessRequest request;
    request.set_task_id(cached_manifest.task_id());
    auto witness_client = raylet_client_pool_->GetOrConnectByAddress(witness);

    witness_client->GetRecoveryWitness(
        std::move(request),
        [state, witness_count, certificate_mode, callback](
            const Status &status,
            rpc::GetRecoveryWitnessReply &&reply) mutable {
          bool finished = false;
          std::optional<rpc::RecoveryManifest> result;
          {
            absl::MutexLock lock(&state->mutex);
            ++state->completed;

            if (status.ok() && reply.found() && reply.has_manifest()) {
              if (!state->merged.has_value()) {
                state->merged = reply.manifest();
              } else if (certificate_mode) {
                rpc::RecoveryManifest merged;
                merged.CopyFrom(state->merged.value());
                if (MergeRecoveryWitnessViews(reply.manifest(), &merged)) {
                  state->merged = std::move(merged);
                }
              } else if (CompareRecoveryManifestVersions(
                             reply.manifest(), state->merged.value()) > 0) {
                state->merged = reply.manifest();
              }
            }

            if (state->completed == witness_count) {
              result = state->merged;
              finished = true;
            }
          }

          if (finished) {
            callback(std::move(result));
          }
        });
  }
}


}  // namespace ray::core
