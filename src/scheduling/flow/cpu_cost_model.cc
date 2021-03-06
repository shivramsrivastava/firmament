/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include "scheduling/flow/cpu_cost_model.h"
#include "scheduling/flow/coco_cost_model.h"

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/label_utils.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/flow_graph_manager.h"

DEFINE_uint64(max_multi_arcs_for_cpu, 50, "Maximum number of multi-arcs.");

DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

CpuCostModel::CpuCostModel(shared_ptr<ResourceMap_t> resource_map,
                           shared_ptr<TaskMap_t> task_map,
                           shared_ptr<KnowledgeBase> knowledge_base)
  : resource_map_(resource_map), task_map_(task_map),
    knowledge_base_(knowledge_base) {
}

ArcDescriptor CpuCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  return ArcDescriptor(2560000, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::TaskToResourceNode(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return ArcDescriptor(0LL, CapacityFromResNodeToParent(destination), 0ULL);
}

ArcDescriptor CpuCostModel::LeafResourceNodeToSink(ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor CpuCostModel::TaskContinuation(TaskID_t task_id) {
  // TODO(ionel): Implement before running with preemption enabled.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::TaskPreemption(TaskID_t task_id) {
  // TODO(ionel): Implement before running with preemption enabled.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                       EquivClass_t ec) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  // The arcs between ECs an machine can only carry unit flow.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  CostVector_t* resource_request = FindOrNull(ec_resource_requirement_, ec1);
  CHECK_NOTNULL(resource_request);
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec2);
  CHECK_NOTNULL(machine_res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, *machine_res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  CostVector_t available_resources;
  available_resources.cpu_cores_ = static_cast<uint32_t>(rd.available_resources().cpu_cores());
  available_resources.ram_cap_ = static_cast<uint32_t>(rd.available_resources().ram_cap());
  uint64_t* index = FindOrNull(ec_to_index_, ec2);
  CHECK_NOTNULL(index);
  uint64_t ec_index = *index;
  if ((available_resources.cpu_cores_ < resource_request->cpu_cores_ * ec_index) ||
      (available_resources.ram_cap_ < resource_request->ram_cap_ * ec_index)) {
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
  available_resources.cpu_cores_ = rd.available_resources().cpu_cores() - ec_index * resource_request->cpu_cores_;
  available_resources.ram_cap_ = rd.available_resources().ram_cap() - ec_index * resource_request->ram_cap_;
  int64_t cpu_cost = ((rd.resource_capacity().cpu_cores() - available_resources.cpu_cores_) / rd.resource_capacity().cpu_cores()) * 1000;
  int64_t ram_cost = ((rd.resource_capacity().ram_cap() - available_resources.ram_cap_) / rd.resource_capacity().ram_cap()) * 1000;
  int64_t cost = cpu_cost + ram_cost;
  return ArcDescriptor(cost, 1ULL, 0ULL);
}

vector<EquivClass_t>* CpuCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  // Get the equivalence class for the resource request: cpu and memory
  vector<EquivClass_t>* ecs = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  CostVector_t* task_resource_request = FindOrNull(task_resource_requirement_, task_id);
  CHECK_NOTNULL(task_resource_request);
  size_t resource_req_selectors_hash = scheduler::HashSelectors(td_ptr->label_selectors());
  boost::hash_combine(resource_req_selectors_hash, task_resource_request->cpu_cores_);
  EquivClass_t resource_request_ec = static_cast<EquivClass_t>(resource_req_selectors_hash);
  ecs->push_back(resource_request_ec);
  InsertIfNotPresent(&ec_resource_requirement_, resource_request_ec, *task_resource_request);
  InsertIfNotPresent(&ec_to_label_selectors, resource_request_ec, 
                     td_ptr->label_selectors());
  return ecs;
}

vector<ResourceID_t>* CpuCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* machine_res = new vector<ResourceID_t>();
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec);
  if (machine_res_id) {
    machine_res->push_back(*machine_res_id);
  }
  return machine_res;
}

vector<ResourceID_t>* CpuCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* pref_res = new vector<ResourceID_t>();
  return pref_res;
}

vector<EquivClass_t>* CpuCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  vector<EquivClass_t>* pref_ecs = new vector<EquivClass_t>();
  CostVector_t* task_resource_request = FindOrNull(ec_resource_requirement_, ec);
  if (task_resource_request) {
    const RepeatedPtrField<LabelSelector>* label_selectors =
      FindOrNull(ec_to_label_selectors, ec);
    CHECK_NOTNULL(label_selectors);
    for (auto& ec_machines : ecs_for_machines_) {
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, ec_machines.first);
      CHECK_NOTNULL(rs);
      const ResourceDescriptor& rd = rs->topology_node().resource_desc();
      if (!scheduler::SatisfiesLabelSelectors(rd, *label_selectors))
        continue;
      CostVector_t available_resources;
      available_resources.cpu_cores_ = static_cast<uint32_t>(rd.available_resources().cpu_cores());
      available_resources.ram_cap_ = static_cast<uint32_t>(rd.available_resources().ram_cap());
      //LOG(INFO) << "Available resources before selecting machine ECs: "
      //          << "Cores: " << available_resources.cpu_cores_ << ", "
      //          << "Memory: " << available_resources.ram_cap_;
      ResourceID_t res_id = ResourceIDFromString(rd.uuid());
      vector<EquivClass_t>* ecs_for_machine =
        FindOrNull(ecs_for_machines_, res_id);
      CHECK_NOTNULL(ecs_for_machine);
      uint64_t index = 0;
      CostVector_t cur_resource;
      for (cur_resource = *task_resource_request;
           cur_resource.cpu_cores_ <= available_resources.cpu_cores_ && cur_resource.ram_cap_ <= available_resources.ram_cap_ && index < ecs_for_machine->size();
           cur_resource.cpu_cores_ += task_resource_request->cpu_cores_ , cur_resource.ram_cap_ += task_resource_request->ram_cap_, index++) {
        pref_ecs->push_back(ec_machines.second[index]);
      }
    }
  }
  //LOG(INFO) << "No. of preferred ecs: " << pref_ecs->size();
  return pref_ecs;
}

void CpuCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  const ResourceDescriptor& rd = rtnd_ptr->resource_desc();
  // Keep track of the new machine
  CHECK(rd.type() == ResourceDescriptor::RESOURCE_MACHINE);
  ResourceID_t res_id = ResourceIDFromString(rd.uuid());
  vector<EquivClass_t> machine_ecs;
  for (uint64_t index = 0; index < FLAGS_max_multi_arcs_for_cpu; ++index) {
    EquivClass_t multi_machine_ec = GetMachineEC(rd.friendly_name(), index);
    machine_ecs.push_back(multi_machine_ec);
    CHECK(InsertIfNotPresent(&ec_to_index_, multi_machine_ec, index));
    CHECK(InsertIfNotPresent(&ec_to_machine_, multi_machine_ec, res_id));
  }
  //LOG(INFO) << "DEBUG: Size of machine ECs for res: " << machine_ecs.size() << " for " << rd.friendly_name();
  CHECK(InsertIfNotPresent(&ecs_for_machines_, res_id, machine_ecs));
}

void CpuCostModel::AddTask(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  CostVector_t resource_request;
  resource_request.cpu_cores_ = static_cast<uint32_t>(td.resource_request().cpu_cores());
  resource_request.ram_cap_ = static_cast<uint32_t>(td.resource_request().ram_cap());
  //LOG(INFO) << "Requested cpu cores at AddTask: " << resource_request.cpu_cores_;
  //LOG(INFO) << "Requested mem at AddTask: " << resource_request.ram_cap_;
  CHECK(InsertIfNotPresent(&task_resource_requirement_, task_id,
                           resource_request));
}

void CpuCostModel::RemoveMachine(ResourceID_t res_id) {
  // vector<EquivClass_t>* ecs = FindOrNull(ecs_for_machines_, res_id);
  // CHECK_NOTNULL(ecs);
  // for (EquivClass_t& ec : *ecs) {
  //   CHECK_EQ(ec_to_machine_.erase(ec), 1);
  //   CHECK_EQ(ec_to_index_.erase(ec), 1);
  // }
  // CHECK_EQ(ecs_for_machines_.erase(res_id), 1);
}

void CpuCostModel::RemoveTask(TaskID_t task_id) {
  // CHECK_EQ(task_rx_bw_requirement_.erase(task_id), 1);
  CHECK_EQ(task_resource_requirement_.erase(task_id), 1);
}

EquivClass_t CpuCostModel::GetMachineEC(const string& machine_name,
                                        uint64_t ec_index) {
  uint64_t hash = HashString(machine_name);
  boost::hash_combine(hash, ec_index);
  return static_cast<EquivClass_t>(hash);
}

FlowGraphNode* CpuCostModel::GatherStats(FlowGraphNode* accumulator,
                                         FlowGraphNode* other) {
  if (!accumulator->IsResourceNode()) {
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    // The other node is not a resource node.
    if (other->type_ == FlowNodeType::SINK) {
      accumulator->rd_ptr_->set_num_running_tasks_below(
          static_cast<uint64_t>(
              accumulator->rd_ptr_->current_running_tasks_size()));
      accumulator->rd_ptr_->set_num_slots_below(FLAGS_max_tasks_per_pu);
    }
    return accumulator;
  }

  CHECK_NOTNULL(other->rd_ptr_);
  ResourceDescriptor* rd_ptr = accumulator->rd_ptr_;
  CHECK_NOTNULL(rd_ptr);
  if (accumulator->type_ == FlowNodeType::MACHINE) {
    // Grab the latest available resource sample from the machine
    ResourceStats latest_stats;
    // Take the most recent sample for now
    bool have_sample =
      knowledge_base_->GetLatestStatsForMachine(accumulator->resource_id_,
                                                &latest_stats);
    if (have_sample) {
      //LOG(INFO) << "DEBUG: Size of cpu stats: " << latest_stats.cpus_stats_size();
      uint32_t core_id = 0;
      float available_cpu_cores = latest_stats.cpus_stats(0).cpu_allocatable();
       // latest_stats.cpus_stats(core_id).cpu_capacity() *
       // (1.0 - latest_stats.cpus_stats(core_id).cpu_utilization());
      //auto available_ram_cap = latest_stats.mem_capacity() *
      auto available_ram_cap = latest_stats.mem_allocatable();
       // (1.0 - latest_stats.mem_utilization());
      //LOG(INFO) << "DEBUG: Stats from latest machine sample: "
      //          << "Available cpu: " << available_cpu_cores << "\n"
      //          << "Available mem: " << available_ram_cap;
      rd_ptr->mutable_available_resources()->set_cpu_cores(
        available_cpu_cores);
      rd_ptr->mutable_available_resources()->set_ram_cap(
        available_ram_cap);
    }
  }

  return accumulator;
}

void CpuCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* CpuCostModel::UpdateStats(FlowGraphNode* accumulator,
                                         FlowGraphNode* other) {
  return accumulator;
}

ResourceID_t CpuCostModel::MachineResIDForResource(ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceTopologyNodeDescriptor* rtnd = rs->mutable_topology_node();
  while (rtnd->resource_desc().type() != ResourceDescriptor::RESOURCE_MACHINE) {
    if (rtnd->parent_id().empty()) {
      LOG(FATAL) << "Non-machine resource " << rtnd->resource_desc().uuid()
                 << " has no parent!";
    }
    rs = FindPtrOrNull(*resource_map_, ResourceIDFromString(rtnd->parent_id()));
    rtnd = rs->mutable_topology_node();
  }
  return ResourceIDFromString(rtnd->resource_desc().uuid());
}

}  // namespace firmament
