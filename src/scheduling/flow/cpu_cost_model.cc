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
#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/flow_graph_manager.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/label_utils.h"

DEFINE_uint64(max_multi_arcs_for_cpu, 50, "Maximum number of multi-arcs.");

DECLARE_uint64(max_tasks_per_pu);
DECLARE_bool(pod_affinity_antiaffinity_symmetry);

namespace firmament {

CpuCostModel::CpuCostModel(
    shared_ptr<ResourceMap_t> resource_map, shared_ptr<TaskMap_t> task_map,
    shared_ptr<KnowledgeBase> knowledge_base,
    unordered_map<string, unordered_map<string, vector<TaskID_t>>>* labels_map)
    : resource_map_(resource_map),
      task_map_(task_map),
      knowledge_base_(knowledge_base),
      labels_map_(labels_map) {
  // Set an initial value for infinity -- this overshoots a bit; would be nice
  // to have a tighter bound based on actual costs observed
  infinity_ = omega_ * CpuMemCostVector_t::dimensions_;
}

void CpuCostModel::AccumulateResourceStats(ResourceDescriptor* accumulator,
                                           ResourceDescriptor* other) {
  // Track the aggregate available resources below the machine node
  ResourceVector* acc_avail = accumulator->mutable_available_resources();
  ResourceVector* other_avail = other->mutable_available_resources();
  acc_avail->set_cpu_cores(acc_avail->cpu_cores() + other_avail->cpu_cores());
  // Running/idle task count
  accumulator->set_num_running_tasks_below(
      accumulator->num_running_tasks_below() +
      other->num_running_tasks_below());
  accumulator->set_num_slots_below(accumulator->num_slots_below() +
                                   other->num_slots_below());
}

pair<TaskID_t, ResourceID_t> CpuCostModel::GetTaskMappingForSingleTask(TaskID_t task_id) {
  // This function returns best matching resource for given taskid.
  vector<EquivClass_t>* ecs = GetTaskEquivClasses(task_id);
  pair<TaskID_t, ResourceID_t> delta;
  delta.first = task_id;
  ResourceID_t* res_id = FindOrNull(ec_to_best_fit_resource_, (*ecs)[0]);
  if (res_id) {
    delta.second = *res_id;
  } else {
#ifdef __PLATFORM_HAS_BOOST__
    delta.second = boost::uuids::nil_uuid();
#else
    delta.second = 0;
#endif
  }
  return delta;
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
    const ResourceDescriptor& source, const ResourceDescriptor& destination) {
  return ArcDescriptor(0LL, CapacityFromResNodeToParent(destination), 0ULL);
}

ArcDescriptor CpuCostModel::LeafResourceNodeToSink(ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor CpuCostModel::TaskContinuation(TaskID_t task_id) {
  // TODO(shivramsrivastava): Implement before running with preemption enabled.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::TaskPreemption(TaskID_t task_id) {
  // TODO(shivramsrivastava): Implement before running with preemption enabled.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                       EquivClass_t ec) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::EquivClassToResourceNode(EquivClass_t ec,
                                                     ResourceID_t res_id) {
  // The arcs between ECs an machine can only carry unit flow.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::EquivClassToEquivClass(EquivClass_t ec1,
                                                   EquivClass_t ec2) {
  CpuMemCostVector_t cost_vector;
  CpuMemResVector_t* resource_request =
      FindOrNull(ec_resource_requirement_, ec1);
  CHECK_NOTNULL(resource_request);
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec2);
  CHECK_NOTNULL(machine_res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, *machine_res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  CpuMemResVector_t available_resources;
  available_resources.cpu_cores_ =
      static_cast<uint64_t>(rd.available_resources().cpu_cores());
  available_resources.ram_cap_ =
      static_cast<uint64_t>(rd.available_resources().ram_cap());
  uint64_t* index = FindOrNull(ec_to_index_, ec2);
  CHECK_NOTNULL(index);
  uint64_t ec_index = *index;
  if ((available_resources.cpu_cores_ <
       resource_request->cpu_cores_ * ec_index) ||
      (available_resources.ram_cap_ < resource_request->ram_cap_ * ec_index)) {
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
  available_resources.cpu_cores_ = rd.available_resources().cpu_cores() -
                                   ec_index * resource_request->cpu_cores_;
  available_resources.ram_cap_ = rd.available_resources().ram_cap() -
                                 ec_index * resource_request->ram_cap_;

  // Expressing Least Requested Priority.
  float cpu_fraction =
      ((rd.resource_capacity().cpu_cores() - available_resources.cpu_cores_) /
       (float)rd.resource_capacity().cpu_cores());
  float ram_fraction =
      ((rd.resource_capacity().ram_cap() - available_resources.ram_cap_) /
       (float)rd.resource_capacity().ram_cap());
  int64_t cpu_cost = cpu_fraction * omega_;
  int64_t ram_cost = ram_fraction * omega_;
  int64_t cpu_ram_cost = (cpu_cost + ram_cost) / 2;
  cost_vector.cpu_mem_cost_ = cpu_ram_cost;

  // Expressing Balanced Resource Allocation Priority.
  // Compute variance for two fractions, cpu and ram.
  float mean = (cpu_fraction + ram_fraction) / float(2);
  float variance = (((cpu_fraction - mean) * (cpu_fraction - mean)) +
                    ((ram_fraction - mean) * (ram_fraction - mean))) /
                   float(2);
  // Since the variance is between positive fractions, it will be positive
  // fraction. Variance lets the cost to be higher for node which has high
  // variance and multiplying it with omega_(1000) provides the scaling
  // factor needed.
  int64_t balanced_resource_cost = variance * omega_;
  cost_vector.balanced_res_cost_ = balanced_resource_cost;

  // Expressing Node Affinity priority.
  const TaskDescriptor* td_ptr = FindOrNull(ec_to_td_requirements, ec1);
  CHECK_NOTNULL(td_ptr);
  int64_t node_affinity_normalized_score = 0;
  if (td_ptr->has_affinity()) {
    const Affinity& affinity = td_ptr->affinity();
    if (affinity.has_node_affinity()) {
      if (affinity.node_affinity()
              .preferredduringschedulingignoredduringexecution_size()) {
        unordered_map<ResourceID_t, PriorityScoresList_t,
                      boost::hash<boost::uuids::uuid>>*
            nodes_priority_scores_ptr =
                FindOrNull(ec_to_node_priority_scores, ec1);
        CHECK_NOTNULL(nodes_priority_scores_ptr);
        PriorityScoresList_t* priority_scores_struct_ptr =
            FindOrNull(*nodes_priority_scores_ptr, *machine_res_id);
        CHECK_NOTNULL(priority_scores_struct_ptr);
        PriorityScore_t& node_affinity_score =
            priority_scores_struct_ptr->node_affinity_priority;
        if (node_affinity_score.satisfy) {
          MinMaxScores_t* max_min_priority_scores =
              FindOrNull(ec_to_max_min_priority_scores, ec1);
          CHECK_NOTNULL(max_min_priority_scores);
          if (node_affinity_score.final_score == -1) {
            // Normalised node affinity score is not calculated for this
            // machine, so calculate and store it once.
            int64_t max_score =
                max_min_priority_scores->node_affinity_priority.max_score;
            if (max_score) {
              node_affinity_normalized_score =
                  (node_affinity_score.score / (float)(max_score)) * omega_;
              node_affinity_score.final_score = node_affinity_normalized_score;
            }
          } else {
            // Normalized node affinity score is already calculated, so use it.
            node_affinity_normalized_score = node_affinity_score.final_score;
          }
        }
      }
    }
  }

  // Expressing pod affinity/anti-affinity priority scores.
  int64_t pod_affinity_normalized_score = 0;
  bool pod_affinity_or_anti_affinity_task = false;
  if (td_ptr->has_affinity() && (td_ptr->affinity().has_pod_affinity() ||
                                 td_ptr->affinity().has_pod_anti_affinity())) {
    pod_affinity_or_anti_affinity_task = true;
  }
  if ((td_ptr->has_affinity() &&
       ((td_ptr->affinity().has_pod_affinity() &&
         td_ptr->affinity()
             .pod_affinity()
             .preferredduringschedulingignoredduringexecution_size()) ||
        (td_ptr->affinity().has_pod_anti_affinity() &&
         td_ptr->affinity()
             .pod_anti_affinity()
             .preferredduringschedulingignoredduringexecution_size()))) ||
      (ecs_with_pod_antiaffinity_symmetry_.find(ec1) !=
       ecs_with_pod_antiaffinity_symmetry_.end())) {
    unordered_map<ResourceID_t, PriorityScoresList_t,
                  boost::hash<boost::uuids::uuid>>* nodes_priority_scores_ptr =
        FindOrNull(ec_to_node_priority_scores, ec1);
    CHECK_NOTNULL(nodes_priority_scores_ptr);
    PriorityScoresList_t* priority_scores_struct_ptr =
        FindOrNull(*nodes_priority_scores_ptr, *machine_res_id);
    CHECK_NOTNULL(priority_scores_struct_ptr);
    PriorityScore_t& pod_affinity_score =
        priority_scores_struct_ptr->pod_affinity_priority;
    MinMaxScores_t* max_min_priority_scores =
        FindOrNull(ec_to_max_min_priority_scores, ec1);
    CHECK_NOTNULL(max_min_priority_scores);
    if (pod_affinity_score.final_score == -1) {
      int64_t max_score =
          max_min_priority_scores->pod_affinity_priority.max_score;
      int64_t min_score =
          max_min_priority_scores->pod_affinity_priority.min_score;
      if ((max_score - min_score) > 0) {
        pod_affinity_normalized_score =
            ((pod_affinity_score.score - min_score) / (max_score - min_score)) *
            omega_;
      }
      pod_affinity_score.final_score = pod_affinity_normalized_score;
    } else {
      pod_affinity_normalized_score = pod_affinity_score.final_score;
    }
  }
  //Expressing taints/tolerations priority scores
    unordered_map<ResourceID_t, PriorityScoresList_t,
                    boost::hash<boost::uuids::uuid>>*
          taints_priority_scores_ptr =
              FindOrNull(ec_to_node_priority_scores, ec1);
      CHECK_NOTNULL(taints_priority_scores_ptr);
      PriorityScoresList_t* priority_scores_struct_ptr =
          FindOrNull(*taints_priority_scores_ptr, *machine_res_id);
      CHECK_NOTNULL(priority_scores_struct_ptr);
      PriorityScore_t& taints_score =
          priority_scores_struct_ptr->intolerable_taints_priority;
      if (taints_score.satisfy) {
        MinMaxScores_t* max_min_priority_scores =
            FindOrNull(ec_to_max_min_priority_scores, ec1);
        CHECK_NOTNULL(max_min_priority_scores);
        if (taints_score.final_score == -1) {
          // Normalised taints score is not calculated for this
          // machine, so calculate and store it once.
          int64_t max_score =
              max_min_priority_scores->intolerable_taints_priority.max_score;
          if (max_score) {
            taints_score.final_score =  (taints_score.score / (float)(max_score)) * omega_;
          }
        }
      }else{

	taints_score.final_score = 0;
     }
  
  cost_vector.node_affinity_soft_cost_ =
      omega_ - node_affinity_normalized_score;
  cost_vector.pod_affinity_soft_cost_ = omega_ - pod_affinity_normalized_score;
  cost_vector.intolerable_taints_cost_ = taints_score.final_score;
  
	
  Cost_t final_cost = FlattenCostVector(cost_vector);
  //Added for solver
  if (pod_affinity_or_anti_affinity_task) {
    ResourceID_t current_resource_id =
      ResourceIDFromString(rs->topology_node().children(0).resource_desc().uuid());
    ResourceID_t* res_id = FindOrNull(ec_to_best_fit_resource_, ec1);
    if (!res_id) {
      ec_to_min_cost_[ec1] = final_cost;
      ec_to_best_fit_resource_[ec1] = current_resource_id;
    } else {
      if (ec_to_min_cost_[ec1] > final_cost) {
        ec_to_min_cost_[ec1] = final_cost;
        ec_to_best_fit_resource_[ec1] = current_resource_id;
      }
    }
  }
  return ArcDescriptor(final_cost, 1ULL, 0ULL);
}

Cost_t CpuCostModel::FlattenCostVector(CpuMemCostVector_t cv) {
  int64_t accumulator = 0;
  accumulator += cv.cpu_mem_cost_;
  accumulator += cv.balanced_res_cost_;
  accumulator += cv.node_affinity_soft_cost_;
  accumulator += cv.pod_affinity_soft_cost_;
  accumulator += cv.intolerable_taints_cost_;
  if (accumulator > infinity_) infinity_ = accumulator + 1;
  return accumulator;
}

bool CpuCostModel::CheckPodAffinityAntiAffinitySymmetryConflict(
    TaskDescriptor* td_ptr) {
  for (auto itr = resource_to_task_symmetry_map_.begin();
       itr != resource_to_task_symmetry_map_.end(); itr++) {
    if (!SatisfiesPodAntiAffinitySymmetry(itr->first, *td_ptr)) return true;
    // Pod affinity/anti-affinity soft conflict check.
    vector<TaskID_t>* tasks =
        FindOrNull(resource_to_task_symmetry_map_, itr->first);
    if (tasks) {
      unordered_multimap<string, string> task_labels;
      for (const auto& label : td_ptr->labels()) {
        task_labels.insert(pair<string, string>(label.key(), label.value()));
      }
      for (auto task_id : *tasks) {
        TaskDescriptor* target_td_ptr = FindPtrOrNull(*task_map_, task_id);
        if (target_td_ptr) {
          if (target_td_ptr->has_affinity()) {
            Affinity affinity = target_td_ptr->affinity();
            if (affinity.has_pod_affinity()) {
              // Target pod affinity hard constraint preference conflict check.
              if (affinity.pod_affinity()
                      .requiredduringschedulingignoredduringexecution_size()) {
                for (auto& affinityterm :
                     affinity.pod_affinity()
                         .requiredduringschedulingignoredduringexecution()) {
                  if (SatisfiesPodAffinitySymmetryTerm(
                          *td_ptr, *target_td_ptr, task_labels, affinityterm)) {
                    return true;
                  }
                }
              }
              // Target pod affinity soft constraint conflict check.
              if (affinity.pod_affinity()
                      .preferredduringschedulingignoredduringexecution_size()) {
                for (auto& weightedaffinityterm :
                     affinity.pod_affinity()
                         .preferredduringschedulingignoredduringexecution()) {
                  if (!weightedaffinityterm.weight()) continue;
                  if (weightedaffinityterm.has_podaffinityterm()) {
                    if (SatisfiesPodAffinitySymmetryTerm(
                            *td_ptr, *target_td_ptr, task_labels,
                            weightedaffinityterm.podaffinityterm())) {
                      return true;
                    }
                  }
                }
              }
            }
            // Target pod anti-affinity soft constraint conflict check.
            if (affinity.has_pod_anti_affinity() &&
                affinity.pod_anti_affinity()
                    .preferredduringschedulingignoredduringexecution_size()) {
              for (auto& weightedantiaffinityterm :
                   affinity.pod_anti_affinity()
                       .preferredduringschedulingignoredduringexecution()) {
                if (!weightedantiaffinityterm.weight()) continue;
                if (weightedantiaffinityterm.has_podaffinityterm()) {
                  if (!SatisfiesPodAntiAffinitySymmetryTerm(
                          *td_ptr, *target_td_ptr, task_labels,
                          weightedantiaffinityterm.podaffinityterm())) {
                    return true;
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return false;
}

vector<EquivClass_t>* CpuCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  // Get the equivalence class for the resource request: cpu and memory
  vector<EquivClass_t>* ecs = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  CpuMemResVector_t* task_resource_request =
      FindOrNull(task_resource_requirement_, task_id);
  CHECK_NOTNULL(task_resource_request);
  size_t task_agg = 0;
  bool pod_antiaffinity_symmetry = false;
  if (td_ptr->has_affinity() || (td_ptr->tolerations_size() > DEFAULT_TOLERATIONS)) {
    // For tasks which has affinity requirements, we hash the job id.
    // TODO(jagadish): This hash has to be handled in an efficient way in
    // future.
    task_agg = HashJobID(*td_ptr);
  } else if (td_ptr->label_selectors_size()) {
    task_agg = scheduler::HashSelectors(td_ptr->label_selectors());
    // And also hash the cpu and mem requests.
    boost::hash_combine(
        task_agg, to_string(task_resource_request->cpu_cores_) + "cpumem" +
                      to_string(task_resource_request->ram_cap_));
  } else {
    if (FLAGS_pod_affinity_antiaffinity_symmetry && td_ptr->labels_size() &&
        CheckPodAffinityAntiAffinitySymmetryConflict(td_ptr)) {
      pod_antiaffinity_symmetry = true;
      task_agg = HashJobID(*td_ptr);
    } else {
      // For other tasks, only hash the cpu and mem requests.
      boost::hash_combine(
          task_agg, to_string(task_resource_request->cpu_cores_) + "cpumem" +
                        to_string(task_resource_request->ram_cap_));
    }
  }
  EquivClass_t resource_request_ec = static_cast<EquivClass_t>(task_agg);
  ecs->push_back(resource_request_ec);
  InsertIfNotPresent(&ec_resource_requirement_, resource_request_ec,
                     *task_resource_request);
  InsertIfNotPresent(&ec_to_td_requirements, resource_request_ec, *td_ptr);
  if (pod_antiaffinity_symmetry) {
    ecs_with_pod_antiaffinity_symmetry_.insert(resource_request_ec);
  }
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

void CpuCostModel::CalculatePrioritiesCost(const EquivClass_t ec,
                                           const ResourceDescriptor& rd) {
  // Calculate priorities cost for node affinity.
  const TaskDescriptor* td = FindOrNull(ec_to_td_requirements, ec);
  CHECK_NOTNULL(td);
  int64_t sum_of_weights = 0;
  if (td->has_affinity()) {
    const Affinity& affinity = td->affinity();
    if (affinity.has_node_affinity()) {
      if (affinity.node_affinity()
              .preferredduringschedulingignoredduringexecution_size()) {
        // Match PreferredDuringSchedulingIgnoredDuringExecution term by term
        for (auto& preferredSchedulingTerm :
             affinity.node_affinity()
                 .preferredduringschedulingignoredduringexecution()) {
          // If weight is zero then skip preferredSchedulingTerm.
          if (!preferredSchedulingTerm.weight()) {
            continue;
          }
          // A null or empty node selector term matches no objects.
          if (!preferredSchedulingTerm.has_preference()) {
            continue;
          }
          if (scheduler::NodeMatchesNodeSelectorTerm(
                  rd, preferredSchedulingTerm.preference())) {
            sum_of_weights += preferredSchedulingTerm.weight();
          }
        }
        // Fill the node priority min, max and actual scores which will
        // be used in cost calculation.
        unordered_map<ResourceID_t, PriorityScoresList_t,
                      boost::hash<boost::uuids::uuid>>*
            nodes_priority_scores_ptr =
                FindOrNull(ec_to_node_priority_scores, ec);
        if (!nodes_priority_scores_ptr) {
          // For this EC, no node to priority scores map exists, so initialize
          // it.
          unordered_map<ResourceID_t, PriorityScoresList_t,
                        boost::hash<boost::uuids::uuid>>
              node_to_priority_scores_map;
          InsertIfNotPresent(&ec_to_node_priority_scores, ec,
                             node_to_priority_scores_map);
          nodes_priority_scores_ptr =
              FindOrNull(ec_to_node_priority_scores, ec);
        }
        CHECK_NOTNULL(nodes_priority_scores_ptr);
        ResourceID_t res_id = ResourceIDFromString(rd.uuid());
        PriorityScoresList_t* priority_scores_struct_ptr =
            FindOrNull(*nodes_priority_scores_ptr, res_id);
        if (!priority_scores_struct_ptr) {
          // Priority scores is empty for this node, so initialize it zero.
          PriorityScoresList_t priority_scores_list;
          InsertIfNotPresent(nodes_priority_scores_ptr, res_id,
                             priority_scores_list);
          priority_scores_struct_ptr =
              FindOrNull(*nodes_priority_scores_ptr, res_id);
        }
        CHECK_NOTNULL(priority_scores_struct_ptr);
        // Store the node affinity min, max and actual priority scores that will
        // be utilized in calculating normalized cost.
        PriorityScore_t& node_affinity_score =
            priority_scores_struct_ptr->node_affinity_priority;
        if (!sum_of_weights) {
          // If machine does not satisfies soft constraint then we flag machine
          // such that cost of omega_ is used in cost calculation.
          node_affinity_score.satisfy = false;
        }
        if (node_affinity_score.satisfy) {
          // Machine satisfies soft constraints.
          // Store the node affinity min, max and actual priority scores.
          node_affinity_score.score = sum_of_weights;
          MinMaxScores_t* max_min_priority_scores =
              FindOrNull(ec_to_max_min_priority_scores, ec);
          if (!max_min_priority_scores) {
            MinMaxScores_t priority_scores_list;
            InsertIfNotPresent(&ec_to_max_min_priority_scores, ec,
                               priority_scores_list);
            max_min_priority_scores =
                FindOrNull(ec_to_max_min_priority_scores, ec);
          }
          MinMaxScore_t& min_max_node_affinity_score =
              max_min_priority_scores->node_affinity_priority;
          if (min_max_node_affinity_score.max_score < sum_of_weights ||
              min_max_node_affinity_score.max_score == -1) {
            min_max_node_affinity_score.max_score = sum_of_weights;
          }
        }
      }
    }
  }
}


//Taints and Tolerations
void CpuCostModel::CalculateIntolerableTaintsCost(const ResourceDescriptor& rd,
                                            const TaskDescriptor* td_ptr,
                                            const EquivClass_t ec ){
 bool IsTolerable = false;
 int64_t intolerable_taint_cost = 0;
 for (const auto& tolerations: td_ptr->tolerations()) {
         if (tolerations.effect() == "PreferNoSchedule" || tolerations.effect() == ""){
                if(tolerations.operator_() == "Exists"){
                               if (tolerations.key() != ""){
                                       InsertIfNotPresent(&tolerationSoftExistsMap,tolerations.key(),tolerations.value());
                               }else{
                                       IsTolerable = true;
                               }

                        }
               else if ((tolerations.operator_() == "Equal") || (tolerations.operator_() == "")){

                               InsertIfNotPresent(&tolerationSoftEqualMap,tolerations.key(),tolerations.value());

                        }
               else{
                               LOG(FATAL) << "Unsupported operator :" << tolerations.operator_();
                               break;
                        }

                }

         }

  if (!IsTolerable){
       for (const auto& taint : rd.taints()){
       if (taint.effect() == "PreferNoSchedule"){
               //If the key does not exist in Exists Map, look for Equal Map for any matching key and value

               if (!ContainsKey(tolerationSoftExistsMap,taint.key())){
                       const string* value = FindOrNull(tolerationSoftEqualMap, taint.key());

                       //If key is found, then value is not NULL
                        if (value != NULL) {
                                //Check if the value matches for the found key
                                       if ((*value) != taint.value()) {
                                         intolerable_taint_cost = intolerable_taint_cost + 1;
                                       }
                        }else{ //If the key is not found, then taint is not tolerable
                                       intolerable_taint_cost = intolerable_taint_cost + 1;
                         }

                       }

               }
       }
  }
     // Fill the intolerable taints priority min, max and actual scores which will
        // be used in cost calculation.
        unordered_map<ResourceID_t, PriorityScoresList_t,
                      boost::hash<boost::uuids::uuid>>*
            taints_priority_scores_ptr =
                FindOrNull(ec_to_node_priority_scores, ec);
        if (!taints_priority_scores_ptr) {
          // For this EC, no node to priority scores map exists, so initialize
          // it.
          unordered_map<ResourceID_t, PriorityScoresList_t,
                        boost::hash<boost::uuids::uuid>>
              node_to_priority_scores_map;
          InsertIfNotPresent(&ec_to_node_priority_scores, ec,
                             node_to_priority_scores_map);
          taints_priority_scores_ptr =
              FindOrNull(ec_to_node_priority_scores, ec);
        }
        CHECK_NOTNULL(taints_priority_scores_ptr);
        ResourceID_t res_id = ResourceIDFromString(rd.uuid());
        PriorityScoresList_t* priority_scores_struct_ptr =
            FindOrNull(*taints_priority_scores_ptr, res_id);
        if (!priority_scores_struct_ptr) {
          // Priority scores is empty for this node, so initialize it zero.
          PriorityScoresList_t priority_scores_list;
          InsertIfNotPresent(taints_priority_scores_ptr, res_id,
                             priority_scores_list);
          priority_scores_struct_ptr =
              FindOrNull(*taints_priority_scores_ptr, res_id);
        }
        CHECK_NOTNULL(priority_scores_struct_ptr);
        // Store the intolerable taints min, max and actual priority scores that will
        // be utilized in calculating normalized cost.
        PriorityScore_t& taints_score =
            priority_scores_struct_ptr->intolerable_taints_priority;
        if (!intolerable_taint_cost) {
          // If machine does not satisfies soft constraint then we flag machine
          // such that cost of omega_ is used in cost calculation.
          taints_score.satisfy = false;
        }
        if (taints_score.satisfy) {
          // Machine satisfies soft constraints.
          // Store the intolerable taints min, max and actual priority scores.
          taints_score.score = intolerable_taint_cost;
          MinMaxScores_t* max_min_priority_scores =
              FindOrNull(ec_to_max_min_priority_scores, ec);
          if (!max_min_priority_scores) {
            MinMaxScores_t priority_scores_list;
            InsertIfNotPresent(&ec_to_max_min_priority_scores, ec,
                               priority_scores_list);
            max_min_priority_scores =
                FindOrNull(ec_to_max_min_priority_scores, ec);
          }
          MinMaxScore_t& min_max_taints_score =
              max_min_priority_scores->intolerable_taints_priority;
          if (min_max_taints_score.max_score < intolerable_taint_cost ||
              min_max_taints_score.max_score == -1) {
            min_max_taints_score.max_score = intolerable_taint_cost;
          }
		}
      }



// Pod affinity/anti-affinity
bool CpuCostModel::MatchExpressionWithPodLabels(
    const ResourceDescriptor& rd, const LabelSelectorRequirement& expression) {
  unordered_map<string, vector<TaskID_t>>* label_values =
      FindOrNull(*labels_map_, expression.key());
  if (label_values) {
    for (auto& value : expression.values()) {
      vector<TaskID_t>* labels_map_tasks = FindOrNull(*label_values, value);
      if (labels_map_tasks) {
        for (auto task_id : *labels_map_tasks) {
          TaskDescriptor* tdp = FindPtrOrNull(*task_map_, task_id);
          if (tdp) {
            if (!HasNamespace(tdp->task_namespace())) continue;
            if (tdp->state() == TaskDescriptor::RUNNING) {
              ResourceID_t pu_res_id =
                  ResourceIDFromString(tdp->scheduled_to_resource());
              ResourceID_t machine_res_id = MachineResIDForResource(pu_res_id);
              ResourceID_t res_id = ResourceIDFromString(rd.uuid());
              if (machine_res_id == res_id) {
                return true;
              }
            }
          }
        }
      }
    }
  }
  return false;
}

bool CpuCostModel::NotMatchExpressionWithPodLabels(
    const ResourceDescriptor& rd, const LabelSelectorRequirement& expression) {
  bool namespace_match = false;
  unordered_map<string, vector<TaskID_t>>* label_values =
      FindOrNull(*labels_map_, expression.key());
  if (label_values) {
    for (auto& value : expression.values()) {
      vector<TaskID_t>* labels_map_tasks = FindOrNull(*label_values, value);
      if (labels_map_tasks) {
        for (auto task_id : *labels_map_tasks) {
          TaskDescriptor* tdp = FindPtrOrNull(*task_map_, task_id);
          if (tdp) {
            if (tdp->state() == TaskDescriptor::RUNNING) {
              ResourceID_t pu_res_id =
                  ResourceIDFromString(tdp->scheduled_to_resource());
              ResourceID_t machine_res_id = MachineResIDForResource(pu_res_id);
              ResourceID_t res_id = ResourceIDFromString(rd.uuid());
              if (machine_res_id == res_id) {
                return false;
              }
            }
          }
          if (HasNamespace(tdp->task_namespace())) namespace_match = true;
        }
      }
    }
  }
  if (namespace_match == false) return false;
  return true;
}

bool CpuCostModel::MatchExpressionKeyWithPodLabels(
    const ResourceDescriptor& rd, const LabelSelectorRequirement& expression) {
  unordered_map<string, vector<TaskID_t>>* label_values =
      FindOrNull(*labels_map_, expression.key());
  if (label_values) {
    for (auto it = label_values->begin(); it != label_values->end(); it++) {
      for (auto task_id : it->second) {
        TaskDescriptor* tdp = FindPtrOrNull(*task_map_, task_id);
        if (tdp) {
          if (!HasNamespace(tdp->task_namespace())) continue;
          if (tdp->state() == TaskDescriptor::RUNNING) {
            ResourceID_t pu_res_id =
                ResourceIDFromString(tdp->scheduled_to_resource());
            ResourceID_t machine_res_id = MachineResIDForResource(pu_res_id);
            ResourceID_t res_id = ResourceIDFromString(rd.uuid());
            if (machine_res_id == res_id) {
              return true;
            }
          }
        }
      }
    }
  }
  return false;
}

bool CpuCostModel::NotMatchExpressionKeyWithPodLabels(
    const ResourceDescriptor& rd, const LabelSelectorRequirement& expression) {
  bool namespace_match = false;
  unordered_map<string, vector<TaskID_t>>* label_values =
      FindOrNull(*labels_map_, expression.key());
  if (label_values) {
    for (auto it = label_values->begin(); it != label_values->end(); it++) {
      for (auto task_id : it->second) {
        TaskDescriptor* tdp = FindPtrOrNull(*task_map_, task_id);
        if (tdp) {
          if (tdp->state() == TaskDescriptor::RUNNING) {
            ResourceID_t pu_res_id =
                ResourceIDFromString(tdp->scheduled_to_resource());
            ResourceID_t machine_res_id = MachineResIDForResource(pu_res_id);
            ResourceID_t res_id = ResourceIDFromString(rd.uuid());
            if (machine_res_id == res_id) {
              return false;
            }
          }
        }
        if (HasNamespace(tdp->task_namespace())) namespace_match = true;
      }
    }
  }
  if (namespace_match == false) return false;
  return true;
}

bool CpuCostModel::SatisfiesPodAntiAffinityMatchExpression(
    const ResourceDescriptor& rd,
    const LabelSelectorRequirementAntiAff& expression) {
  LabelSelectorRequirement expression_selector;
  expression_selector.set_key(expression.key());
  expression_selector.set_operator_(expression.operator_());
  for (auto& value : expression.values()) {
    expression_selector.add_values(value);
  }
  if (expression.operator_() == std::string("In")) {
    if (!MatchExpressionWithPodLabels(rd, expression_selector)) return true;
  } else if (expression.operator_() == std::string("NotIn")) {
    if (!NotMatchExpressionWithPodLabels(rd, expression_selector)) return true;
  } else if (expression.operator_() == std::string("Exists")) {
    if (!MatchExpressionKeyWithPodLabels(rd, expression_selector)) return true;
  } else if (expression.operator_() == std::string("DoesNotExist")) {
    if (!NotMatchExpressionKeyWithPodLabels(rd, expression_selector))
      return true;
  } else {
    LOG(FATAL) << "Unsupported selector type: " << expression.operator_();
    return false;
  }
  return false;
}

bool CpuCostModel::SatisfiesPodAffinityMatchExpression(
    const ResourceDescriptor& rd, const LabelSelectorRequirement& expression) {
  if (expression.operator_() == std::string("In")) {
    if (MatchExpressionWithPodLabels(rd, expression)) return true;
  } else if (expression.operator_() == std::string("NotIn")) {
    if (NotMatchExpressionWithPodLabels(rd, expression)) return true;
  } else if (expression.operator_() == std::string("Exists")) {
    if (MatchExpressionKeyWithPodLabels(rd, expression)) return true;
  } else if (expression.operator_() == std::string("DoesNotExist")) {
    if (NotMatchExpressionKeyWithPodLabels(rd, expression)) return true;
  } else {
    LOG(FATAL) << "Unsupported selector type: " << expression.operator_();
    return false;
  }
  return false;
}

bool CpuCostModel::SatisfiesPodAntiAffinityMatchExpressions(
    const ResourceDescriptor& rd,
    const RepeatedPtrField<LabelSelectorRequirementAntiAff>& matchexpressions) {
  for (auto& expression : matchexpressions) {
    if (SatisfiesPodAntiAffinityMatchExpression(rd, expression)) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

bool CpuCostModel::SatisfiesPodAffinityMatchExpressions(
    const ResourceDescriptor& rd,
    const RepeatedPtrField<LabelSelectorRequirement>& matchexpressions) {
  for (auto& expression : matchexpressions) {
    if (SatisfiesPodAffinityMatchExpression(rd, expression)) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

bool CpuCostModel::SatisfiesPodAntiAffinityTerm(
    const ResourceDescriptor& rd, const TaskDescriptor& td,
    const PodAffinityTermAntiAff& term) {
  if (!term.namespaces_size()) {
    namespaces.insert(td.task_namespace());
  } else {
    for (auto name : term.namespaces()) {
      namespaces.insert(name);
    }
  }
  if (term.has_labelselector()) {
    if (term.labelselector().matchexpressions_size()) {
      if (!SatisfiesPodAntiAffinityMatchExpressions(
              rd, term.labelselector().matchexpressions()))
        return false;
    }
  }
   namespaces.clear();
  return true;
}

bool CpuCostModel::SatisfiesPodAffinityTerm(const ResourceDescriptor& rd,
                                            const TaskDescriptor& td,
                                            const PodAffinityTerm& term) {
  if (!term.namespaces_size()) {
    namespaces.insert(td.task_namespace());
  } else {
    for (auto name : term.namespaces()) {
      namespaces.insert(name);
    }
  }
  if (term.has_labelselector()) {
    if (term.labelselector().matchexpressions_size()) {
      if (!SatisfiesPodAffinityMatchExpressions(
              rd, term.labelselector().matchexpressions()))
        return false;
    }
  }
  namespaces.clear();
  return true;
}

bool CpuCostModel::SatisfiesPodAntiAffinityTerms(
    const ResourceDescriptor& rd, const TaskDescriptor& td,
    const RepeatedPtrField<PodAffinityTermAntiAff>& podantiaffinityterms) {
  for (auto& term : podantiaffinityterms) {
    if (!SatisfiesPodAntiAffinityTerm(rd, td, term)) return false;
  }
  return true;
}

bool CpuCostModel::SatisfiesPodAffinityTerms(
    const ResourceDescriptor& rd, const TaskDescriptor& td,
    const RepeatedPtrField<PodAffinityTerm>& podaffinityterms) {
  for (auto& term : podaffinityterms) {
    if (!SatisfiesPodAffinityTerm(rd, td, term)) return false;
  }
  return true;
}

// Hard constraint check for pod affinity/anti-affinity.
bool CpuCostModel::SatisfiesPodAffinityAntiAffinityRequired(
    const ResourceDescriptor& rd, const TaskDescriptor& td,
    const EquivClass_t ec) {
  // Check for pod anti-affinity symmetry.
  if (FLAGS_pod_affinity_antiaffinity_symmetry &&
      (ecs_with_pod_antiaffinity_symmetry_.find(ec) !=
       ecs_with_pod_antiaffinity_symmetry_.end())) {
    if (td.labels_size()) {
      if (!SatisfiesPodAntiAffinitySymmetry(ResourceIDFromString(rd.uuid()),
                                            td)) {
        return false;
      }
    }
  }

  if (td.has_affinity()) {
    Affinity affinity = td.affinity();
    if (affinity.has_pod_anti_affinity()) {
      if (affinity.pod_anti_affinity()
              .requiredduringschedulingignoredduringexecution_size()) {
        if (!SatisfiesPodAntiAffinityTerms(
                rd, td,
                affinity.pod_anti_affinity()
                    .requiredduringschedulingignoredduringexecution())) {
          return false;
        }
      }
    }
    if (affinity.has_pod_affinity()) {
      if (affinity.pod_affinity()
              .requiredduringschedulingignoredduringexecution_size()) {
        if (!SatisfiesPodAffinityTerms(
                rd, td,
                affinity.pod_affinity()
                    .requiredduringschedulingignoredduringexecution())) {
          return false;
        }
      }
    }
  }
  return true;
}

int64_t CpuCostModel::CalculatePodAffinitySymmetryPreference(
    Affinity affinity, const TaskDescriptor& td,
    const TaskDescriptor& target_td,
    unordered_multimap<string, string> task_labels) {
  int64_t sum_of_weights = 0;
  if (affinity.has_pod_affinity()) {
    // Target pod affinity hard constraint score.
    if (affinity.pod_affinity()
            .requiredduringschedulingignoredduringexecution_size()) {
      for (auto& affinityterm :
           affinity.pod_affinity()
               .requiredduringschedulingignoredduringexecution()) {
        if (SatisfiesPodAffinitySymmetryTerm(td, target_td, task_labels,
                                             affinityterm)) {
          sum_of_weights += 100;
        }
      }
    }
    // Target pod affinity soft constraint score.
    if (affinity.pod_affinity()
            .preferredduringschedulingignoredduringexecution_size()) {
      for (auto& weightedaffinityterm :
           affinity.pod_affinity()
               .preferredduringschedulingignoredduringexecution()) {
        if (!weightedaffinityterm.weight()) continue;
        if (weightedaffinityterm.has_podaffinityterm()) {
          if (SatisfiesPodAffinitySymmetryTerm(
                  td, target_td, task_labels,
                  weightedaffinityterm.podaffinityterm())) {
            sum_of_weights += weightedaffinityterm.weight();
          }
        }
      }
    }
  }
  return sum_of_weights;
}

int64_t CpuCostModel::CalculatePodAntiAffinitySymmetryPreference(
    Affinity affinity, const TaskDescriptor& td,
    const TaskDescriptor& target_td,
    unordered_multimap<string, string> task_labels) {
  int64_t sum_of_weights = 0;
  // Target pod anti-affinity soft constraint score.
  if (affinity.has_pod_anti_affinity() &&
      affinity.pod_anti_affinity()
          .preferredduringschedulingignoredduringexecution_size()) {
    for (auto& weightedantiaffinityterm :
         affinity.pod_anti_affinity()
             .preferredduringschedulingignoredduringexecution()) {
      if (!weightedantiaffinityterm.weight()) continue;
      if (weightedantiaffinityterm.has_podaffinityterm()) {
        if (!SatisfiesPodAntiAffinitySymmetryTerm(
                td, target_td, task_labels,
                weightedantiaffinityterm.podaffinityterm())) {
          sum_of_weights += weightedantiaffinityterm.weight();
        }
      }
    }
  }
  return sum_of_weights;
}


// Soft constraint check for pod affinity/anti-affinity.
void CpuCostModel::CalculatePodAffinityAntiAffinityPreference(
    const ResourceDescriptor& rd, const TaskDescriptor& td,
    const EquivClass_t ec) {
  int64_t sum_of_weights = 0;
  // Pod affinity/anti-affinity symmetry.
  if (FLAGS_pod_affinity_antiaffinity_symmetry &&
      (ecs_with_pod_antiaffinity_symmetry_.find(ec) !=
       ecs_with_pod_antiaffinity_symmetry_.end())) {
    if (td.labels_size()) {
      unordered_multimap<string, string> task_labels;
      for (const auto& label : td.labels()) {
        task_labels.insert(pair<string, string>(label.key(), label.value()));
      }
      vector<TaskID_t>* tasks = FindOrNull(resource_to_task_symmetry_map_,
                                           ResourceIDFromString(rd.uuid()));
      if (tasks) {
        for (auto task_id : *tasks) {
          TaskDescriptor* target_td_ptr = FindPtrOrNull(*task_map_, task_id);
          if (target_td_ptr) {
            if (target_td_ptr->has_affinity()) {
              Affinity affinity = target_td_ptr->affinity();
              sum_of_weights += CalculatePodAffinitySymmetryPreference(
                  affinity, td, *target_td_ptr, task_labels);
              sum_of_weights -= CalculatePodAntiAffinitySymmetryPreference(
                  affinity, td, *target_td_ptr, task_labels);
            }
          }
        }
      }
    }
  }

  if (td.has_affinity()) {
    Affinity affinity = td.affinity();
    if (affinity.has_pod_anti_affinity()) {
      if (affinity.pod_anti_affinity()
              .preferredduringschedulingignoredduringexecution_size()) {
        for (auto& weightedpodantiaffinityterm :
             affinity.pod_anti_affinity()
                 .preferredduringschedulingignoredduringexecution()) {
          if (!weightedpodantiaffinityterm.weight()) continue;
          if (weightedpodantiaffinityterm.has_podaffinityterm()) {
            if (SatisfiesPodAntiAffinityTerm(
                    rd, td, weightedpodantiaffinityterm.podaffinityterm())) {
              sum_of_weights += weightedpodantiaffinityterm.weight();
            }
          }
        }
      }
    }
    if (affinity.has_pod_affinity()) {
      if (affinity.pod_affinity()
              .preferredduringschedulingignoredduringexecution_size()) {
        for (auto& weightedpodaffinityterm :
             affinity.pod_affinity()
                 .preferredduringschedulingignoredduringexecution()) {
          if (!weightedpodaffinityterm.weight()) continue;
          if (weightedpodaffinityterm.has_podaffinityterm()) {
            if (SatisfiesPodAffinityTerm(
                    rd, td, weightedpodaffinityterm.podaffinityterm())) {
              sum_of_weights += weightedpodaffinityterm.weight();
            }
          }
        }
      }
    }
  }
  unordered_map<ResourceID_t, PriorityScoresList_t,
                boost::hash<boost::uuids::uuid>>* nodes_priority_scores_ptr =
      FindOrNull(ec_to_node_priority_scores, ec);
  if (!nodes_priority_scores_ptr) {
    unordered_map<ResourceID_t, PriorityScoresList_t,
                  boost::hash<boost::uuids::uuid>>
        node_to_priority_scores_map;
    InsertIfNotPresent(&ec_to_node_priority_scores, ec,
                       node_to_priority_scores_map);
    nodes_priority_scores_ptr = FindOrNull(ec_to_node_priority_scores, ec);
  }
  CHECK_NOTNULL(nodes_priority_scores_ptr);
  ResourceID_t res_id = ResourceIDFromString(rd.uuid());
  PriorityScoresList_t* priority_scores_struct_ptr =
      FindOrNull(*nodes_priority_scores_ptr, res_id);
  if (!priority_scores_struct_ptr) {
    PriorityScoresList_t priority_scores_list;
    InsertIfNotPresent(nodes_priority_scores_ptr, res_id,
                       priority_scores_list);
    priority_scores_struct_ptr =
        FindOrNull(*nodes_priority_scores_ptr, res_id);
  }
  CHECK_NOTNULL(priority_scores_struct_ptr);
  PriorityScore_t& pod_affinity_score =
      priority_scores_struct_ptr->pod_affinity_priority;
  if (!sum_of_weights) {
    pod_affinity_score.satisfy = false;
  }
  pod_affinity_score.score = sum_of_weights;
  MinMaxScores_t* max_min_priority_scores =
      FindOrNull(ec_to_max_min_priority_scores, ec);
  if (!max_min_priority_scores) {
    MinMaxScores_t priority_scores_list;
    InsertIfNotPresent(&ec_to_max_min_priority_scores, ec,
                       priority_scores_list);
    max_min_priority_scores = FindOrNull(ec_to_max_min_priority_scores, ec);
  }
  CHECK_NOTNULL(max_min_priority_scores);
  MinMaxScore_t& min_max_pod_affinity_score =
      max_min_priority_scores->pod_affinity_priority;
  if (min_max_pod_affinity_score.max_score < sum_of_weights ||
      min_max_pod_affinity_score.max_score == -1) {
    min_max_pod_affinity_score.max_score = sum_of_weights;
  }
  if (min_max_pod_affinity_score.min_score > sum_of_weights ||
      min_max_pod_affinity_score.min_score == -1) {
    min_max_pod_affinity_score.min_score = sum_of_weights;
  }
}

// Pod affinity/anti-affinity symmetry.
void CpuCostModel::UpdateResourceToTaskSymmetryMap(ResourceID_t res_id,
                                                   TaskID_t task_id) {
  ResourceID_t machine_res_id = MachineResIDForResource(res_id);
  vector<TaskID_t>* tasks =
      FindOrNull(resource_to_task_symmetry_map_, machine_res_id);
  if (tasks) {
    tasks->push_back(task_id);
  } else {
    vector<TaskID_t> task_vec;
    task_vec.push_back(task_id);
    InsertIfNotPresent(&resource_to_task_symmetry_map_, machine_res_id,
                       task_vec);
  }
}

void CpuCostModel::RemoveTaskFromTaskSymmetryMap(TaskDescriptor* td_ptr) {
  ResourceID_t res_id = ResourceIDFromString(td_ptr->scheduled_to_resource());
  ResourceID_t machine_res_id = MachineResIDForResource(res_id);
  vector<TaskID_t>* tasks =
      FindOrNull(resource_to_task_symmetry_map_, machine_res_id);
  if (tasks) {
    auto it = find(tasks->begin(), tasks->end(), td_ptr->uid());
    if (it != tasks->end()) {
      tasks->erase(it);
    }
    if (!tasks->size()) {
      resource_to_task_symmetry_map_.erase(machine_res_id);
    }
  }
}

void CpuCostModel::RemoveECFromPodSymmetryMap(EquivClass_t ec) {
  ecs_with_pod_antiaffinity_symmetry_.erase(ec);
}

bool CpuCostModel::SatisfiesSymmetryMatchExpression(
    unordered_multimap<string, string> task_labels,
    LabelSelectorRequirement expression_selector) {
  auto range_it = task_labels.equal_range(expression_selector.key());
  if (expression_selector.operator_() == std::string("In")) {
    for (auto it = range_it.first; it != range_it.second; it++) {
      for (auto value : expression_selector.values()) {
        if (it->second == value) {
          return true;
        }
      }
    }
    return false;
  } else if (expression_selector.operator_() == std::string("NotIn")) {
    for (auto it = range_it.first; it != range_it.second; it++) {
      for (auto value : expression_selector.values()) {
        if (it->second == value) {
          return false;
        }
      }
    }
    return true;
  } else if (expression_selector.operator_() == std::string("Exists")) {
    return (range_it.first != range_it.second);
  } else if (expression_selector.operator_() == std::string("DoesNotExist")) {
    return !(range_it.first != range_it.second);
  } else {
    LOG(FATAL) << "Unsupported selector type: "
               << expression_selector.operator_();
  }
  return false;
}

bool CpuCostModel::SatisfiesPodAffinitySymmetryMatchExpressions(
    unordered_multimap<string, string> task_labels,
    const RepeatedPtrField<LabelSelectorRequirement>& matchexpressions) {
  for (auto& expression_selector : matchexpressions) {
    if (SatisfiesSymmetryMatchExpression(task_labels, expression_selector)) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

bool CpuCostModel::SatisfiesPodAntiAffinitySymmetryMatchExpressions(
    unordered_multimap<string, string> task_labels,
    const RepeatedPtrField<LabelSelectorRequirementAntiAff>& matchexpressions) {
  for (auto& expression : matchexpressions) {
    LabelSelectorRequirement expression_selector;
    expression_selector.set_key(expression.key());
    expression_selector.set_operator_(expression.operator_());
    for (auto& value : expression.values()) {
      expression_selector.add_values(value);
    }
    if (!SatisfiesSymmetryMatchExpression(task_labels, expression_selector)) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

bool CpuCostModel::SatisfiesPodAffinitySymmetryTerm(
    const TaskDescriptor& td, const TaskDescriptor& target_td,
    unordered_multimap<string, string> task_labels,
    const PodAffinityTerm& term) {
  if (!term.namespaces_size()) {
    namespaces.insert(target_td.task_namespace());
  } else {
    for (auto name : term.namespaces()) {
      namespaces.insert(name);
    }
  }
  if (!HasNamespace(td.task_namespace())) {
    return false;
  }
  if (term.has_labelselector()) {
    if (term.labelselector().matchexpressions_size()) {
      if (!SatisfiesPodAffinitySymmetryMatchExpressions(
              task_labels, term.labelselector().matchexpressions())) {
        return false;
      }
    }
  }
  namespaces.clear();
  return true;
}

bool CpuCostModel::SatisfiesPodAntiAffinitySymmetryTerm(
    const TaskDescriptor& td, const TaskDescriptor& target_td,
    unordered_multimap<string, string> task_labels,
    const PodAffinityTermAntiAff term) {
  if (!term.namespaces_size()) {
    namespaces.insert(target_td.task_namespace());
  } else {
    for (auto name : term.namespaces()) {
      namespaces.insert(name);
    }
  }
  if (HasNamespace(td.task_namespace())) {
    return false;
  }
  if (term.has_labelselector()) {
    if (term.labelselector().matchexpressions_size()) {
      if (!SatisfiesPodAntiAffinitySymmetryMatchExpressions(
              task_labels, term.labelselector().matchexpressions())) {
        return false;
      }
    }
  }
  namespaces.clear();
  return true;
}

bool CpuCostModel::SatisfiesPodAntiAffinityTermsSymmetry(
    const TaskDescriptor& td, const TaskDescriptor& target_td,
    unordered_multimap<string, string> task_labels,
    const RepeatedPtrField<PodAffinityTermAntiAff>& podantiaffinityterms) {
  for (auto& term : podantiaffinityterms) {
    if (!SatisfiesPodAntiAffinitySymmetryTerm(td, target_td, task_labels,
                                              term)) {
      return false;
    }
  }
  return true;
}

// Hard constraint check for pod anti-affinity symmetry.
bool CpuCostModel::SatisfiesPodAntiAffinitySymmetry(ResourceID_t res_id,
                                                    const TaskDescriptor& td) {
  unordered_multimap<string, string> task_labels;
  for (const auto& label : td.labels()) {
    task_labels.insert(pair<string, string>(label.key(), label.value()));
  }
  vector<TaskID_t>* tasks = FindOrNull(resource_to_task_symmetry_map_, res_id);
  if (tasks) {
    for (auto task_id : *tasks) {
      TaskDescriptor* target_td_ptr = FindPtrOrNull(*task_map_, task_id);
      if (target_td_ptr) {
        if (target_td_ptr->has_affinity()) {
          Affinity affinity = target_td_ptr->affinity();
          if (affinity.has_pod_anti_affinity() &&
              affinity.pod_anti_affinity()
                  .requiredduringschedulingignoredduringexecution_size()) {
            if (!SatisfiesPodAntiAffinityTermsSymmetry(
                    td, *target_td_ptr, task_labels,
                    affinity.pod_anti_affinity()
                        .requiredduringschedulingignoredduringexecution())) {
              return false;
            }
          }
        }
      }
    }
  }
  return true;
}

vector<EquivClass_t>* CpuCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  vector<EquivClass_t>* pref_ecs = new vector<EquivClass_t>();
  CpuMemResVector_t* task_resource_request =
      FindOrNull(ec_resource_requirement_, ec);
  if (task_resource_request) {
    // Clear priority scores for node affinity and pod affinity.
    // TODO(jagadish): Currently we clear old affinity scores, and restore new
    // scores. But we are not clearing it just after scheduling round completed,
    // we are clearing in the subsequent scheduling round, need to improve this.
    ec_to_node_priority_scores.clear();
	//Added to clear the map before filling the values for each node
	tolerationSoftEqualMap.clear();
	tolerationSoftExistsMap.clear();
    for (auto& ec_machines : ecs_for_machines_) {
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, ec_machines.first);
      CHECK_NOTNULL(rs);
      const ResourceDescriptor& rd = rs->topology_node().resource_desc();
      const TaskDescriptor* td_ptr = FindOrNull(ec_to_td_requirements, ec);
      if (td_ptr) {
        // Checking whether machine satisfies node selector and node affinity.
        if (scheduler::SatisfiesNodeSelectorAndNodeAffinity(rd, *td_ptr)) {
          // Calculate costs for all priorities.
          CalculatePrioritiesCost(ec, rd);
        } else
          continue;
        // Checking pod affinity/anti-affinity
        if (SatisfiesPodAffinityAntiAffinityRequired(rd, *td_ptr, ec)) {
          CalculatePodAffinityAntiAffinityPreference(rd, *td_ptr, ec);
        } else {
          continue;
        }
		//Check whether taints in the machine has matching tolerations
		if (scheduler::HasMatchingTolerationforNodeTaints(rd, *td_ptr)) {
		  CalculateIntolerableTaintsCost(rd, td_ptr, ec);
		}else {
                 continue;
        }
		// Checking costs for intolerable taints
		
      }
      CpuMemResVector_t available_resources;
      available_resources.cpu_cores_ =
          static_cast<uint64_t>(rd.available_resources().cpu_cores());
      available_resources.ram_cap_ =
          static_cast<uint64_t>(rd.available_resources().ram_cap());
      ResourceID_t res_id = ResourceIDFromString(rd.uuid());
      vector<EquivClass_t>* ecs_for_machine =
          FindOrNull(ecs_for_machines_, res_id);
      CHECK_NOTNULL(ecs_for_machine);
      uint64_t index = 0;
      CpuMemResVector_t cur_resource;
      uint64_t task_count = rd.num_running_tasks_below();
      //TODO(dujun) : FLAGS_max_tasks_per_pu is treated as equivalent to max-pods,
      // as max-pods functionality is not yet merged at this point.
      for (cur_resource = *task_resource_request;
           cur_resource.cpu_cores_ <= available_resources.cpu_cores_ &&
           cur_resource.ram_cap_ <= available_resources.ram_cap_ &&
           index < ecs_for_machine->size() && task_count < FLAGS_max_tasks_per_pu;
           cur_resource.cpu_cores_ += task_resource_request->cpu_cores_,
          cur_resource.ram_cap_ += task_resource_request->ram_cap_, index++, task_count++) {
        pref_ecs->push_back(ec_machines.second[index]);
      }
    }
  }
  return pref_ecs;
}

void CpuCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
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
  CHECK(InsertIfNotPresent(&ecs_for_machines_, res_id, machine_ecs));
}

void CpuCostModel::AddTask(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  CpuMemResVector_t resource_request;
  resource_request.cpu_cores_ =
      static_cast<uint64_t>(td.resource_request().cpu_cores());
  resource_request.ram_cap_ =
      static_cast<uint64_t>(td.resource_request().ram_cap());
  CHECK(InsertIfNotPresent(&task_resource_requirement_, task_id,
                           resource_request));
}

void CpuCostModel::RemoveMachine(ResourceID_t res_id) {
  vector<EquivClass_t>* ecs = FindOrNull(ecs_for_machines_, res_id);
  CHECK_NOTNULL(ecs);
  for (EquivClass_t& ec : *ecs) {
    CHECK_EQ(ec_to_machine_.erase(ec), 1);
    CHECK_EQ(ec_to_index_.erase(ec), 1);
  }
  CHECK_EQ(ecs_for_machines_.erase(res_id), 1);
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
  if (accumulator->type_ == FlowNodeType::COORDINATOR) {
    return accumulator;
  }
  ResourceDescriptor* rd_ptr = accumulator->rd_ptr_;
  CHECK_NOTNULL(rd_ptr);
  if (accumulator->type_ == FlowNodeType::PU) {
    CHECK(other->resource_id_.is_nil());
    ResourceStats latest_stats;
    ResourceID_t machine_res_id =
        MachineResIDForResource(accumulator->resource_id_);
    bool have_sample = knowledge_base_->GetLatestStatsForMachine(machine_res_id,
                                                                 &latest_stats);
    if (have_sample) {
      VLOG(2) << "Updating PU " << accumulator->resource_id_ << "'s "
              << "resource stats!";
      // Get the CPU stats for this PU
      string label = rd_ptr->friendly_name();
      uint64_t idx = label.find("PU #");
      if (idx != string::npos) {
        string core_id_substr = label.substr(idx + 4, label.size() - idx - 4);
        uint32_t core_id = strtoul(core_id_substr.c_str(), 0, 10);
        float available_cpu_cores =
            latest_stats.cpus_stats(core_id).cpu_capacity() *
            (1.0 - latest_stats.cpus_stats(core_id).cpu_utilization());
        rd_ptr->mutable_available_resources()->set_cpu_cores(
            available_cpu_cores);
      }
      // Running/idle task count
      rd_ptr->set_num_running_tasks_below(rd_ptr->current_running_tasks_size());
      rd_ptr->set_num_slots_below(FLAGS_max_tasks_per_pu);
      return accumulator;
    }
  } else if (accumulator->type_ == FlowNodeType::MACHINE) {
    // Grab the latest available resource sample from the machine
    ResourceStats latest_stats;
    // Take the most recent sample for now
    bool have_sample = knowledge_base_->GetLatestStatsForMachine(
        accumulator->resource_id_, &latest_stats);
    if (have_sample) {
      VLOG(2) << "Updating machine " << accumulator->resource_id_ << "'s "
              << "resource stats!";
      rd_ptr->mutable_available_resources()->set_ram_cap(
          latest_stats.mem_capacity() * (1.0 - latest_stats.mem_utilization()));
    }
    if (accumulator->rd_ptr_ && other->rd_ptr_) {
      AccumulateResourceStats(accumulator->rd_ptr_, other->rd_ptr_);
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
  accumulator->rd_ptr_->clear_available_resources();
  // Clear maps related to priority scores.
  ec_to_node_priority_scores.clear();
  ec_to_min_cost_.clear();
  ec_to_best_fit_resource_.clear();
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
