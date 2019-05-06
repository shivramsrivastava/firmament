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

  inline unordered_map<string, Resource_Allocated>*
                               GetPGToResourceAllocated() {
    return &pg_to_resource_allocated_map_;
  }

  inline unordered_map<JobID_t, string, boost::hash<JobID_t>>*
      GetJobIdToPodGroupMap() {
    return &job_id_to_pod_group_map_;
  }

  inline unordered_map<string, ArcCost_t>* GetPodGroupToArcCost() {
    return &pod_group_to_Arc_cost_;
  }

  inline void ClearPodGroupToArcCost() { pod_group_to_Arc_cost_.clear(); }

  inline unordered_map<string, Queue_Proportion>* GetQueueMapToProportion() {
    return &queue_map_proportion_;
  }

  inline unordered_map<string, string>* GetPodGroupToQueue() {
    return &pod_group_to_queue_map_;
  }

  inline unordered_map<string, list<string>>* GetQtoOrderedPgListMap() {
    return &queue_to_ordered_pg_list_map_;
  }

  inline void ClearQtoOrderedPgListMap() { queue_to_ordered_pg_list_map_.clear(); }

  inline unordered_map<string, PodGroupDescriptor>* GetPodGroupMap() {
    return &pod_group_map_;
  }

  static Firmament_Scheduler_Service_Utils* instance_;

 private:
  Firmament_Scheduler_Service_Utils() {}
  ~Firmament_Scheduler_Service_Utils() {}

  unordered_map<string, Queue_Proportion> queue_map_proportion_;
  unordered_map<string, string> pod_group_to_queue_map_;
  unordered_map<string, Resource_Allocated> pg_to_resource_allocated_map_;
  unordered_map<JobID_t, string, boost::hash<JobID_t>> job_id_to_pod_group_map_;
  unordered_map<string, ArcCost_t> pod_group_to_Arc_cost_;
  unordered_map<string, list<string>> queue_to_ordered_pg_list_map_;
  unordered_map<string, PodGroupDescriptor> pod_group_map_;
};

} // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_SERVICE_UTILS_H
