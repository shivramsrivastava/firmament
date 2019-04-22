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

#include "scheduling/proportion_queue.h"
#include "misc/map-util.h"
#include "base/types.h"

namespace firmament {

class Firmament_Scheduler_Service_Utils {

private:
	
	Firmament_Scheduler_Service_Utils() {}
	~Firmament_Scheduler_Service_Utils(){}

public:
	static Firmament_Scheduler_Service_Utils* Instance()	{
		//*** TBD need to make to thread safe
		if(instance_ == NULL) {
			instance_ =  new Firmament_Scheduler_Service_Utils;
		}
		return instance_;
	}

	/*
	inline boost::shared_ptr<QueueMap_t>* GetQueueMap() {
		return &queue_map_;
		}
;
	inline unordered_map<string, double>* GetQueueMapRatio() {
		return &queue_map_ratio_;
		}
		
	inline unordered_map<string, double>* GetQueueMapProportion() {
		return &queue_map_Proportion_;
		}

	inline unordered_map<string, double>* GetPodGroupMap() {
		return &pod_group_map_;
		}
	*/
	unordered_map<string, Resource_Allocated>* GetPGToResourceAllocated() {
		return &pg_to_resource_allocated_map_;
		}

unordered_map<TaskID_t, string>* GetTaskToPodGroupMap() {
	return &task_to_pod_group_map_;
	}


unordered_map<string, ArcCost_t>* GetPodGroupToArcCost() {
		return &pod_group_to_Arc_cost_;
		}

	static Firmament_Scheduler_Service_Utils* instance_;

private:

	
	/*
	//multi tenant support throug Queue
  boost::shared_ptr<QueueMap_t>queue_map_; 
  unordered_map<string, double> queue_map_ratio_;
  unordered_map<string, Queue_Proportion> queue_map_Proportion_;
	//map for pod group
	unordered_map<string, PodGroupDescriptor> pod_group_map_;
	unordered_map<string, string> pod_group_to_queue_map_;

*/
	unordered_map<string, Resource_Allocated> pg_to_resource_allocated_map_;
	unordered_map<TaskID_t, string> task_to_pod_group_map_;
	unordered_map<string, ArcCost_t> pod_group_to_Arc_cost_;
	
protected:
	//add protected members here
};

//Firmament_Scheduler_Service_Utils* Firmament_Scheduler_Service_Utils::instance_ =  NULL;
}

#endif // FIRMAMENT_SCHEDULING_SERVICE_UTILS_H
