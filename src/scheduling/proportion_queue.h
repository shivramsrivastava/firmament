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

// Coordinator knowledege base. This implements data structures and management
// code for the performance and utilization data reported by tasks and other
// coordinators.

#ifndef FIRMAMENT_SCHEDULING_QUEUE_H
#define FIRMAMENT_SCHEDULING_QUEUE_H

#include<iostream>
using namespace std;

namespace firmament {

typedef unsigned long long uInt64_t;

struct Resources_Available {
  float cpu_resource;
  uInt64_t memory_resource;
  uInt64_t ephemeral_resource;

  Resources_Available() {
    cout<<"*** initiling cpu_res mem and ephemeral res"<<endl;
    cpu_resource = 0.0;
    memory_resource = 0;
    ephemeral_resource = 0;
  }
  float GetCpuResource() {
    return cpu_resource;
  }
  uInt64_t GetMemoryResource() {
    return memory_resource;
  }

  uInt64_t GetEphimeralResource() {
    return ephemeral_resource;
  }

  void SetResources(float cpuRes, uInt64_t memoryRes,
                    uInt64_t epheRes) {
    cpu_resource = cpuRes;
    memory_resource = memoryRes;
    ephemeral_resource = epheRes;
  }

  void AddResources(float cpuRes, uInt64_t memoryRes,
                    uInt64_t epheRes) {
    cpu_resource += cpuRes;
    memory_resource += memoryRes;
    ephemeral_resource += epheRes;
    }

  void DeductResources(float cpuRes, uInt64_t memoryRes,
                    uInt64_t epheRes) {
    cpu_resource -= cpuRes;
    memory_resource -= memoryRes;
    ephemeral_resource -= epheRes;
    }


};
struct ResourceStatsAggregate {
  Resources_Available resource_capcity;
  Resources_Available resource_allocatable;
};

typedef struct Resources_Available Resource_Allocated;

class Queue_Proportion {
 public:
  Queue_Proportion();
  ~Queue_Proportion();
  void SetDeservedResource(float cpuRes, uInt64_t memoryRes, uInt64_t epheRes);
  void SetAllocatedResource(float cpuRes, uInt64_t memoryRes, uInt64_t epheRes);
  void SetRequestedResource(float cpuRes, uInt64_t memoryRes, uInt64_t epheRes);
  void AddAllocatedResource(float cpuRes, uInt64_t memoryRes, uInt64_t epheRes);
  void AddRequestedResource(float cpuRes, uInt64_t memoryRes, uInt64_t epheRes);
  void DeductAllocatedResource(float cpuRes, uInt64_t memoryRes,
                               uInt64_t epheRes);
  void DeductRequestedResource(float cpuRes, uInt64_t memoryRes,
                               uInt64_t epheRes);
  Resources_Available GetDeservedResource();
  Resources_Available GetAllocatedResource();
  Resources_Available GetRequestedResource();

 private:
  Resources_Available deserved_resource_;
  Resources_Available allocated_resource_;
  Resources_Available requested_resource_;
};

}  // end of name space firmament
#endif
