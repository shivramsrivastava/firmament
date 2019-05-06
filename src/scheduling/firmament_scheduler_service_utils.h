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

#ifndef FIRMAMENT_SCHEDULING_SERVICE_UTILS_H
#define FIRMAMENT_SCHEDULING_SERVICE_UTILS_H

#include "base/types.h"
#include "misc/map-util.h"
#include "scheduling/proportion_queue.h"

namespace firmament {

typedef unordered_map<string, std::multimap<uint64_t, string>>
    Qname_To_Cost_PG_Multi_Map;

class Firmament_Scheduler_Service_Utils {
 private:
  Firmament_Scheduler_Service_Utils() {
    queue_map_proportion_.clear();
    pod_group_to_queue_map_.clear();
    pg_to_resource_allocated_map_.clear();
    job_id_to_pod_group_map_.clear();
    q_to_ordered_pg_list_map_.clear();
    pod_group_map_.clear();
  }
  ~Firmament_Scheduler_Service_Utils() {}

 public:
  static Firmament_Scheduler_Service_Utils* Instance() {
    if (instance_ == NULL) {
      instance_ = new Firmament_Scheduler_Service_Utils;
    }
    return instance_;
  }

  static void RemoveInstance() {
    if (instance_ != NULL) {
      delete instance_;
      instance_ = NULL;
    }
  }

  unordered_map<string, Resource_Allocated>* GetPGToResourceAllocated() {
    return &pg_to_resource_allocated_map_;
  }

  unordered_map<JobID_t, string, boost::hash<JobID_t>>*
      GetJobIdToPodGroupMap() {
    return &job_id_to_pod_group_map_;
  }

  unordered_map<string, ArcCost_t>* GetPodGroupToArcCost() {
    return &pod_group_to_Arc_cost_;
  }

  void ClearPodGroupToArcCost() { pod_group_to_Arc_cost_.clear(); }
  unordered_map<string, Queue_Proportion>* GetQueueMapToProportion() {
    return &queue_map_proportion_;
  }

  unordered_map<string, string>* GetPodGroupToQueue() {
    return &pod_group_to_queue_map_;
  }

  unordered_map<string, list<string>>* GetQtoOrderedPgListMap() {
    return &q_to_ordered_pg_list_map_;
  }

  void ClearQtoOrderedPgListMap() { q_to_ordered_pg_list_map_.clear();}

  unordered_map<string, PodGroupDescriptor>* GetPodGroupMap() {
    return &pod_group_map_;
  }

  static Firmament_Scheduler_Service_Utils* instance_;//*** TBD

 private:

  unordered_map<string, Queue_Proportion> queue_map_proportion_;
  unordered_map<string, string> pod_group_to_queue_map_;
  unordered_map<string, Resource_Allocated> pg_to_resource_allocated_map_;
  // remove map task_to_pod_group_map_ added new
  // unordered_map<TaskID_t, string> task_to_pod_group_map_;
  unordered_map<JobID_t, string, boost::hash<JobID_t>> job_id_to_pod_group_map_;
  unordered_map<string, ArcCost_t> pod_group_to_Arc_cost_;
  // Qname_To_Cost_PG_Multi_Map q_map_to_cost_pod_group_multi_map_;
  // Pg is ordered based on the cost
  //  list<string> ordered_pg_list_;
  unordered_map<string, list<string>> q_to_ordered_pg_list_map_;
  unordered_map<string, PodGroupDescriptor> pod_group_map_;


 protected:
  // add protected members here
};

}

#endif  // FIRMAMENT_SCHEDULING_SERVICE_UTILS_H

