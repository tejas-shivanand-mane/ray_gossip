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

#include "ray/raylet_rpc_client/raylet_client.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/bundle_spec.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"

namespace ray {
namespace rpc {

namespace {
uint64_t RecoveryWitnessClientProfileNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
}  // namespace

RayletClient::RayletClient(const rpc::Address &address,
                           rpc::ClientCallManager &client_call_manager,
                           std::function<void()> raylet_unavailable_timeout_callback)
    : grpc_client_(std::make_shared<rpc::GrpcClient<rpc::NodeManagerService>>(
          address.ip_address(), address.port(), client_call_manager)),
      retryable_grpc_client_(rpc::RetryableGrpcClient::Create(
          grpc_client_->Channel(),
          client_call_manager.GetMainService(),
          /*max_pending_requests_bytes=*/std::numeric_limits<uint64_t>::max(),
          /*check_channel_status_interval_milliseconds=*/
          ::RayConfig::instance()
              .grpc_client_check_connection_status_interval_milliseconds(),
          /*server_reconnect_timeout_base_seconds=*/
          ::RayConfig::instance().raylet_rpc_server_reconnect_timeout_base_s(),
          /*server_reconnect_timeout_max_seconds=*/
          ::RayConfig::instance().raylet_rpc_server_reconnect_timeout_max_s(),
          /*server_unavailable_timeout_callback=*/
          std::move(raylet_unavailable_timeout_callback),
          /*server_name=*/std::string("Raylet ") + address.ip_address())),
      pins_in_flight_(std::make_shared<std::atomic<int64_t>>(0)) {}

void RayletClient::RequestWorkerLease(
    rpc::RequestWorkerLeaseRequest &&request,
    const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) {
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            RequestWorkerLease,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::PrestartWorkers(
    const rpc::PrestartWorkersRequest &request,
    const rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback) {
  INVOKE_RPC_CALL(NodeManagerService,
                  PrestartWorkers,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

std::shared_ptr<grpc::Channel> RayletClient::GetChannel() const {
  return grpc_client_->Channel();
}

void RayletClient::ReportWorkerBacklog(const rpc::ReportWorkerBacklogRequest &request) {
  INVOKE_RPC_CALL(
      NodeManagerService,
      ReportWorkerBacklog,
      request,
      [](const Status &status, rpc::ReportWorkerBacklogReply &&reply /*unused*/) {
        RAY_LOG_IF_ERROR(INFO, status)
            << "Error reporting lease backlog information: " << status;
      },
      grpc_client_,
      /*method_timeout_ms*/ -1);
}

void RayletClient::ReturnWorkerLease(int worker_port,
                                     const LeaseID &lease_id,
                                     bool disconnect_worker,
                                     const std::string &disconnect_worker_error_detail,
                                     bool worker_exiting) {
  rpc::ReturnWorkerLeaseRequest request;
  request.set_worker_port(worker_port);
  request.set_lease_id(lease_id.Binary());
  request.set_disconnect_worker(disconnect_worker);
  request.set_disconnect_worker_error_detail(disconnect_worker_error_detail);
  request.set_worker_exiting(worker_exiting);
  INVOKE_RETRYABLE_RPC_CALL(
      retryable_grpc_client_,
      NodeManagerService,
      ReturnWorkerLease,
      request,
      [](const Status &status, rpc::ReturnWorkerLeaseReply &&reply /*unused*/) {
        RAY_LOG_IF_ERROR(INFO, status) << "Error returning worker: " << status;
      },
      grpc_client_,
      /*method_timeout_ms*/ -1);
}

void RayletClient::GetWorkerFailureCause(
    const LeaseID &lease_id,
    const ray::rpc::ClientCallback<ray::rpc::GetWorkerFailureCauseReply> &callback) {
  rpc::GetWorkerFailureCauseRequest request;
  request.set_lease_id(lease_id.Binary());
  INVOKE_RPC_CALL(
      NodeManagerService,
      GetWorkerFailureCause,
      request,
      [callback](const Status &status, rpc::GetWorkerFailureCauseReply &&reply) {
        RAY_LOG_IF_ERROR(INFO, status) << "Error getting task result: " << status;
        callback(status, std::move(reply));
      },
      grpc_client_,
      /*method_timeout_ms*/ -1);
}

void RayletClient::RegisterMutableObjectReader(
    const ObjectID &writer_object_id,
    int64_t num_readers,
    const ObjectID &reader_object_id,
    const ray::rpc::ClientCallback<ray::rpc::RegisterMutableObjectReply> &callback) {
  rpc::RegisterMutableObjectRequest request;
  request.set_writer_object_id(writer_object_id.Binary());
  request.set_num_readers(num_readers);
  request.set_reader_object_id(reader_object_id.Binary());
  INVOKE_RPC_CALL(NodeManagerService,
                  RegisterMutableObject,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::PushMutableObject(
    const ObjectID &writer_object_id,
    uint64_t data_size,
    uint64_t metadata_size,
    void *data,
    void *metadata,
    const ray::rpc::ClientCallback<ray::rpc::PushMutableObjectReply> &callback) {
  // Ray sets the gRPC max payload size to ~512 MiB. We set the max chunk size to a
  // slightly lower value to allow extra padding just in case.
  uint64_t kMaxGrpcPayloadSize = RayConfig::instance().max_grpc_message_size() * 0.98;
  uint64_t total_num_chunks = data_size / kMaxGrpcPayloadSize;
  // If `data_size` is not a multiple of `kMaxGrpcPayloadSize`, then we need to send an
  // extra chunk with the remaining data.
  if (data_size % kMaxGrpcPayloadSize) {
    total_num_chunks++;
  }

  for (uint64_t i = 0; i < total_num_chunks; i++) {
    rpc::PushMutableObjectRequest request;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_total_data_size(data_size);
    request.set_total_metadata_size(metadata_size);

    uint64_t chunk_size = (i < total_num_chunks - 1) ? kMaxGrpcPayloadSize
                                                     : (data_size % kMaxGrpcPayloadSize);
    uint64_t offset = i * kMaxGrpcPayloadSize;
    request.set_offset(offset);
    request.set_chunk_size(chunk_size);
    request.set_data(static_cast<char *>(data) + offset, chunk_size);
    // Set metadata for each message so on the receiver side
    // metadata from any message can be used.
    request.set_metadata(static_cast<char *>(metadata), metadata_size);

    // TODO(jackhumphries): Add failure recovery, retries, and timeout.
    INVOKE_RPC_CALL(
        NodeManagerService,
        PushMutableObject,
        request,
        [callback](const Status &status, rpc::PushMutableObjectReply &&reply) {
          RAY_LOG_IF_ERROR(ERROR, status) << "Error pushing mutable object: " << status;
          if (reply.done()) {
            // The callback is only executed once the receiver node receives all chunks
            // for the mutable object write.
            callback(status, std::move(reply));
          }
        },
        grpc_client_,
        /*method_timeout_ms*/ -1);
  }
}

void RayletClient::ReleaseUnusedActorWorkers(
    const std::vector<WorkerID> &workers_in_use,
    const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) {
  rpc::ReleaseUnusedActorWorkersRequest request;
  for (auto &worker_id : workers_in_use) {
    request.add_worker_ids_in_use(worker_id.Binary());
  }
  INVOKE_RPC_CALL(
      NodeManagerService,
      ReleaseUnusedActorWorkers,
      request,
      [callback](const Status &status, rpc::ReleaseUnusedActorWorkersReply &&reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING)
              << "Error releasing workers from raylet, the raylet may have died:"
              << status;
        }
        callback(status, std::move(reply));
      },
      grpc_client_,
      /*method_timeout_ms*/ -1);
}

void RayletClient::CancelWorkerLease(
    const LeaseID &lease_id,
    const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) {
  rpc::CancelWorkerLeaseRequest request;
  request.set_lease_id(lease_id.Binary());
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            CancelWorkerLease,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::PrepareBundleResources(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
    const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback) {
  rpc::PrepareBundleResourcesRequest request;
  std::set<std::string> nodes;
  for (const auto &bundle_spec : bundle_specs) {
    nodes.insert(bundle_spec->NodeId().Hex());
    auto message_bundle = request.add_bundle_specs();
    message_bundle->CopyFrom(bundle_spec->GetMessage());
  }
  RAY_CHECK(nodes.size() == 1);
  INVOKE_RPC_CALL(NodeManagerService,
                  PrepareBundleResources,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::CommitBundleResources(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
    const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback) {
  rpc::CommitBundleResourcesRequest request;
  std::set<std::string> nodes;
  for (const auto &bundle_spec : bundle_specs) {
    nodes.insert(bundle_spec->NodeId().Hex());
    auto message_bundle = request.add_bundle_specs();
    message_bundle->CopyFrom(bundle_spec->GetMessage());
  }
  RAY_CHECK(nodes.size() == 1);
  INVOKE_RPC_CALL(NodeManagerService,
                  CommitBundleResources,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::RemovePlacementGroupBundles(
    const PlacementGroupID &placement_group_id,
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
    const ray::rpc::ClientCallback<ray::rpc::RemovePlacementGroupBundlesReply>
        &callback) {
  RAY_CHECK(!bundle_specs.empty());
  rpc::RemovePlacementGroupBundlesRequest request;
  request.set_placement_group_id(placement_group_id.Binary());
  const auto &expected_node_id = bundle_specs.front()->NodeId();
  for (const auto &bundle_spec : bundle_specs) {
    RAY_CHECK(bundle_spec->NodeId() == expected_node_id)
        << "All bundles in a RemovePlacementGroupBundles batch must target the "
           "same node.";
    auto *message_bundle = request.add_bundle_specs();
    message_bundle->CopyFrom(bundle_spec->GetMessage());
  }
  INVOKE_RPC_CALL(NodeManagerService,
                  RemovePlacementGroupBundles,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::ReleaseUnusedBundles(
    const std::vector<rpc::Bundle> &bundles_in_use,
    const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) {
  rpc::ReleaseUnusedBundlesRequest request;
  for (auto &bundle : bundles_in_use) {
    request.add_bundles_in_use()->CopyFrom(bundle);
  }
  INVOKE_RETRYABLE_RPC_CALL(
      retryable_grpc_client_,
      NodeManagerService,
      ReleaseUnusedBundles,
      request,
      [callback](const Status &status, rpc::ReleaseUnusedBundlesReply &&reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING)
              << "Error releasing bundles from raylet, the raylet may have died:"
              << status;
        }
        callback(status, std::move(reply));
      },
      grpc_client_,
      /*method_timeout_ms*/ -1);
}

void RayletClient::PinObjectIDs(
    const rpc::Address &caller_address,
    const std::vector<ObjectID> &object_ids,
    const ObjectID &generator_id,
    const rpc::ClientCallback<rpc::PinObjectIDsReply> &callback) {
  rpc::PinObjectIDsRequest request;
  request.mutable_owner_address()->CopyFrom(caller_address);
  for (const ObjectID &object_id : object_ids) {
    request.add_object_ids(object_id.Binary());
  }
  if (!generator_id.IsNil()) {
    request.set_generator_id(generator_id.Binary());
  }

  // NOTE: this callback can execute after the RayletClient instance is destroyed, so
  // we capture the shared_ptr to `pins_in_flight_` instead of `this`.
  pins_in_flight_->fetch_add(1);
  auto rpc_callback = [callback, pins_in_flight = pins_in_flight_](
                          Status status, rpc::PinObjectIDsReply &&reply) {
    pins_in_flight->fetch_sub(1);
    callback(status, std::move(reply));
  };
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            PinObjectIDs,
                            request,
                            rpc_callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::ShutdownRaylet(
    const NodeID &node_id,
    bool graceful,
    const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback) {
  rpc::ShutdownRayletRequest request;
  request.set_graceful(graceful);
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            ShutdownRaylet,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::DrainRaylet(
    const rpc::autoscaler::DrainNodeReason &reason,
    const std::string &reason_message,
    int64_t deadline_timestamp_ms,
    const rpc::ClientCallback<rpc::DrainRayletReply> &callback) {
  rpc::DrainRayletRequest request;
  request.set_reason(reason);
  request.set_reason_message(reason_message);
  request.set_deadline_timestamp_ms(deadline_timestamp_ms);
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            DrainRaylet,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::ResizeLocalResourceInstances(
    google::protobuf::Map<std::string, double> resources,
    const rpc::ClientCallback<rpc::ResizeLocalResourceInstancesReply> &callback) {
  rpc::ResizeLocalResourceInstancesRequest request;
  *request.mutable_resources() = std::move(resources);
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            ResizeLocalResourceInstances,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::IsLocalWorkerDead(
    const WorkerID &worker_id,
    const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) {
  rpc::IsLocalWorkerDeadRequest request;
  request.set_worker_id(worker_id.Binary());
  INVOKE_RPC_CALL(NodeManagerService,
                  IsLocalWorkerDead,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::GlobalGC(const rpc::ClientCallback<rpc::GlobalGCReply> &callback) {
  rpc::GlobalGCRequest request;
  INVOKE_RPC_CALL(NodeManagerService,
                  GlobalGC,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::GetResourceLoad(
    const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) {
  rpc::GetResourceLoadRequest request;
  INVOKE_RPC_CALL(NodeManagerService,
                  GetResourceLoad,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::CancelLeasesWithResourceShapes(
    const std::vector<google::protobuf::Map<std::string, double>> &resource_shapes,
    const rpc::ClientCallback<rpc::CancelLeasesWithResourceShapesReply> &callback) {
  rpc::CancelLeasesWithResourceShapesRequest request;

  for (const auto &resource_shape : resource_shapes) {
    rpc::CancelLeasesWithResourceShapesRequest::ResourceShape *resource_shape_proto =
        request.add_resource_shapes();
    resource_shape_proto->mutable_resource_shape()->insert(resource_shape.begin(),
                                                           resource_shape.end());
  }

  INVOKE_RPC_CALL(NodeManagerService,
                  CancelLeasesWithResourceShapes,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::NotifyGCSRestart(
    const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback) {
  rpc::NotifyGCSRestartRequest request;
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            NotifyGCSRestart,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::GetSystemConfig(
    const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback) {
  rpc::GetSystemConfigRequest request;
  INVOKE_RPC_CALL(NodeManagerService,
                  GetSystemConfig,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::GetNodeStats(
    const rpc::GetNodeStatsRequest &request,
    const rpc::ClientCallback<rpc::GetNodeStatsReply> &callback) {
  INVOKE_RPC_CALL(NodeManagerService,
                  GetNodeStats,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

void RayletClient::GetWorkerPIDs(
    const rpc::OptionalItemCallback<std::vector<int32_t>> &callback, int64_t timeout_ms) {
  rpc::GetWorkerPIDsRequest request;
  auto client_callback = [callback](const Status &status,
                                    rpc::GetWorkerPIDsReply &&reply) {
    if (status.ok()) {
      std::vector<int32_t> workers(reply.pids().begin(), reply.pids().end());
      callback(status, workers);
    } else {
      callback(status, std::nullopt);
    }
  };
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            GetWorkerPIDs,
                            request,
                            client_callback,
                            grpc_client_,
                            timeout_ms);
}

void RayletClient::GetAgentPIDs(
    const rpc::OptionalItemCallback<std::vector<int32_t>> &callback, int64_t timeout_ms) {
  rpc::GetAgentPIDsRequest request;
  auto client_callback = [callback](const Status &status,
                                    rpc::GetAgentPIDsReply &&reply) {
    if (status.ok()) {
      std::vector<int32_t> agents;
      if (reply.has_dashboard_agent_pid()) {
        agents.push_back(reply.dashboard_agent_pid());
      }
      if (reply.has_runtime_env_agent_pid()) {
        agents.push_back(reply.runtime_env_agent_pid());
      }
      callback(status, agents);
    } else {
      callback(status, std::nullopt);
    }
  };
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            GetAgentPIDs,
                            request,
                            client_callback,
                            grpc_client_,
                            timeout_ms);
}

void RayletClient::KillLocalActor(
    const rpc::KillLocalActorRequest &request,
    const rpc::ClientCallback<rpc::KillLocalActorReply> &callback) {
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            KillLocalActor,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::CancelLocalTask(
    const rpc::CancelLocalTaskRequest &request,
    const rpc::ClientCallback<rpc::CancelLocalTaskReply> &callback) {
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            CancelLocalTask,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);
}

void RayletClient::FreeLocalObjects(const rpc::FreeLocalObjectsRequest &request) {
  INVOKE_RETRYABLE_RPC_CALL(
      retryable_grpc_client_,
      NodeManagerService,
      FreeLocalObjects,
      request,
      [](const Status &status, rpc::FreeLocalObjectsReply &&reply /*unused*/) {
        if (!status.ok()) {
          RAY_LOG(WARNING)
              << "Error freeing local objects from raylet, the raylet may have died: "
              << status;
        }
      },
      grpc_client_,
      /*method_timeout_ms*/ -1);
}


void RayletClient::UpdateRecoveryWitness(
    rpc::UpdateRecoveryWitnessRequest &&request,
    const rpc::ClientCallback<rpc::UpdateRecoveryWitnessReply> &callback) {
  const bool profiling =
      ::RayConfig::instance().enable_recovery_succession_profiling();
  const uint64_t enqueue_time_ns =
      profiling ? RecoveryWitnessClientProfileNowNs() : 0;
  const uint64_t enqueue_cpu_start_ns =
      profiling ? RecoveryWitnessClientProfileNowNs() : 0;

  PendingRecoveryWitnessUpdate item{std::move(request), callback};
  item.profiling = profiling;
  item.enqueue_time_ns = enqueue_time_ns;

  auto state = recovery_witness_batch_state_;
  std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch;

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->in_flight) {
      state->pending.emplace_back(std::move(item));
      if (profiling) {
        state->pending.back().client_enqueue_cpu_time_ns =
            RecoveryWitnessClientProfileNowNs() - enqueue_cpu_start_ns;
      }
      return;
    }

    state->in_flight = true;
    batch = std::make_shared<std::vector<PendingRecoveryWitnessUpdate>>();
    batch->reserve(1);
    batch->emplace_back(std::move(item));
  }

  if (profiling) {
    batch->back().client_enqueue_cpu_time_ns =
        RecoveryWitnessClientProfileNowNs() - enqueue_cpu_start_ns;
  }
  DispatchRecoveryWitnessBatch(state, grpc_client_, std::move(batch));
}

void RayletClient::DispatchRecoveryWitnessBatch(
    std::shared_ptr<RecoveryWitnessBatchState> state,
    std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client,
    std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch) {
  RAY_CHECK(batch != nullptr && !batch->empty());

  rpc::UpdateRecoveryWitnessBatchRequest request;
  bool profile_batch = false;
  for (const auto &item : *batch) {
    profile_batch = profile_batch || item.profiling;
  }
  const uint64_t dispatch_ns =
      profile_batch ? RecoveryWitnessClientProfileNowNs() : 0;

  if (profile_batch) {
    for (auto &item : *batch) {
      if (item.profiling && item.enqueue_time_ns != 0) {
        item.client_queue_time_ns = dispatch_ns - item.enqueue_time_ns;
      }
    }
  }

  const uint64_t batch_build_cpu_start_ns =
      profile_batch ? RecoveryWitnessClientProfileNowNs() : 0;
  for (auto &item : *batch) {
    rpc::UpdateRecoveryWitnessRequest *update = request.add_updates();
    // The pending request is dead after envelope construction. Move its protobuf
    // storage directly into the physical batch for both Fixed-R and adaptive
    // Succession instead of deep-copying every adaptive witness update.
    update->Swap(&item.request);
  }
  if (batch_build_cpu_start_ns != 0) {
    const uint64_t amortized_batch_build_cpu_ns =
        (RecoveryWitnessClientProfileNowNs() - batch_build_cpu_start_ns) /
        batch->size();
    for (auto &item : *batch) {
      if (item.profiling) {
        item.client_batch_build_cpu_time_ns =
            amortized_batch_build_cpu_ns;
      }
    }
  }

  auto batch_callback =
      [state, grpc_client, batch](
          const Status &status,
          rpc::UpdateRecoveryWitnessBatchReply &&reply) mutable {
        const bool reply_shape_ok =
            !status.ok() ||
            static_cast<size_t>(reply.replies_size()) == batch->size();
        const uint64_t batch_callback_entry_ns =
            reply.client_main_loop_start_time_ns() != 0
                ? RecoveryWitnessClientProfileNowNs()
                : 0;
        const uint64_t client_submit_to_cq_ns =
            reply.client_submit_time_ns() != 0 &&
                    reply.client_cq_receive_time_ns() >=
                        reply.client_submit_time_ns()
                ? reply.client_cq_receive_time_ns() -
                      reply.client_submit_time_ns()
                : 0;
        const uint64_t client_cq_to_main_loop_ns =
            reply.client_cq_receive_time_ns() != 0 &&
                    reply.client_main_loop_start_time_ns() >=
                        reply.client_cq_receive_time_ns()
                ? reply.client_main_loop_start_time_ns() -
                      reply.client_cq_receive_time_ns()
                : 0;
        const uint64_t client_main_loop_to_batch_callback_ns =
            batch_callback_entry_ns != 0 &&
                    batch_callback_entry_ns >=
                        reply.client_main_loop_start_time_ns()
                ? batch_callback_entry_ns -
                      reply.client_main_loop_start_time_ns()
                : 0;
        const uint64_t amortized_witness_handler_ns =
            status.ok() && !batch->empty() &&
                    reply.witness_batch_handler_time_ns() != 0
                ? reply.witness_batch_handler_time_ns() / batch->size()
                : 0;

        if (status.ok() && !reply_shape_ok) {
          RAY_LOG(ERROR)
              << "Recovery witness batch reply size mismatch: sent="
              << batch->size() << " received=" << reply.replies_size();
        }

        for (size_t i = 0; i < batch->size(); ++i) {
          const uint64_t batch_demux_cpu_start_ns =
              (*batch)[i].profiling
                  ? RecoveryWitnessClientProfileNowNs()
                  : 0;
          rpc::UpdateRecoveryWitnessReply item_reply;
          if (status.ok() && reply_shape_ok) {
            item_reply.Swap(reply.mutable_replies(static_cast<int>(i)));
          }
          const uint64_t batch_demux_cpu_time_ns =
              batch_demux_cpu_start_ns != 0
                  ? RecoveryWitnessClientProfileNowNs() -
                        batch_demux_cpu_start_ns
                  : 0;
          // Transport failures retain their non-OK status. A malformed
          // successful batch reply yields the default stored=false item reply,
          // which safely fails that logical witness update.
          if ((*batch)[i].profiling) {
            item_reply.set_client_queue_time_ns((*batch)[i].client_queue_time_ns);
            if (amortized_witness_handler_ns != 0) {
              item_reply.set_witness_handler_time_ns(
                  amortized_witness_handler_ns);
            }
            item_reply.set_client_batch_size(
                static_cast<uint32_t>(batch->size()));
            item_reply.set_client_batch_leader(i == 0);
            item_reply.set_client_submit_to_cq_time_ns(
                client_submit_to_cq_ns);
            item_reply.set_client_cq_to_main_loop_time_ns(
                client_cq_to_main_loop_ns);
            item_reply.set_client_main_loop_to_batch_callback_time_ns(
                client_main_loop_to_batch_callback_ns);
            item_reply.set_client_enqueue_cpu_time_ns(
                (*batch)[i].client_enqueue_cpu_time_ns);
            item_reply.set_client_batch_build_cpu_time_ns(
                (*batch)[i].client_batch_build_cpu_time_ns);
            item_reply.set_client_batch_demux_cpu_time_ns(
                batch_demux_cpu_time_ns);
          }
          (*batch)[i].callback(status, std::move(item_reply));
        }

        auto next_batch =
            std::make_shared<std::vector<PendingRecoveryWitnessUpdate>>();
        {
          std::lock_guard<std::mutex> lock(state->mutex);
          const size_t count = std::min(
              RayletClient::kRecoveryWitnessBatchMaxSize,
              state->pending.size());

          if (count == 0) {
            state->in_flight = false;
            return;
          }

          next_batch->reserve(count);
          for (size_t i = 0; i < count; ++i) {
            next_batch->emplace_back(std::move(state->pending.front()));
            state->pending.pop_front();
          }
        }

        RayletClient::DispatchRecoveryWitnessBatch(
            state, grpc_client, std::move(next_batch));
      };

  INVOKE_RPC_CALL(NodeManagerService,
                  UpdateRecoveryWitnessBatch,
                  request,
                  batch_callback,
                  grpc_client,
                  /*method_timeout_ms=*/-1);
}

void RayletClient::GetRecoveryWitness(
    rpc::GetRecoveryWitnessRequest &&request,
    const rpc::ClientCallback<rpc::GetRecoveryWitnessReply> &callback) {
  INVOKE_RPC_CALL(NodeManagerService,
                  GetRecoveryWitness,
                  request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms=*/-1);
}




}  // namespace rpc
}  // namespace ray
