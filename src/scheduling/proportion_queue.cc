#include "scheduling/proportion_queue.h"

namespace firmament {

Queue_Proportion::Queue_Proportion() {}
Queue_Proportion::~Queue_Proportion() {}
void Queue_Proportion::SetDeservedResource(float cpuRes, uInt64_t memoryRes,
                                           uInt64_t epheRes) {
  deserved_resource_.cpu_resource = cpuRes;
  deserved_resource_.memory_resource = memoryRes;
  deserved_resource_.ephemeral_resource = epheRes;
}

void Queue_Proportion::SetAllocatedResource(float cpuRes, uInt64_t memoryRes,
                                            uInt64_t epheRes) {
  allocated_resource_.cpu_resource = cpuRes;
  allocated_resource_.memory_resource = memoryRes;
  allocated_resource_.ephemeral_resource = epheRes;
}

void Queue_Proportion::SetRequestedResource(float cpuRes, uInt64_t memoryRes,
                                            uInt64_t epheRes) {
  requested_resource_.cpu_resource = cpuRes;
  requested_resource_.memory_resource = memoryRes;
  requested_resource_.ephemeral_resource = epheRes;
}

void Queue_Proportion::AddAllocatedResource(float cpuRes, uInt64_t memoryRes,
                                            uInt64_t epheRes) {
  allocated_resource_.cpu_resource += cpuRes;
  allocated_resource_.memory_resource += memoryRes;
  allocated_resource_.ephemeral_resource += epheRes;
}

void Queue_Proportion::AddRequestedResource(float cpuRes, uInt64_t memoryRes,
                                            uInt64_t epheRes) {
  requested_resource_.cpu_resource += cpuRes;
  requested_resource_.memory_resource += memoryRes;
  requested_resource_.ephemeral_resource += epheRes;
}
void Queue_Proportion::DeductAllocatedResource(float cpuRes, uInt64_t memoryRes,
                                               uInt64_t epheRes) {
  allocated_resource_.cpu_resource -= cpuRes;
  allocated_resource_.memory_resource -= memoryRes;
  allocated_resource_.ephemeral_resource -= epheRes;
}
void Queue_Proportion::DeductRequestedResource(float cpuRes, uInt64_t memoryRes,
                                               uInt64_t epheRes) {
  requested_resource_.cpu_resource -= cpuRes;
  requested_resource_.memory_resource -= memoryRes;
  requested_resource_.ephemeral_resource -= epheRes;
}

Resources_Available Queue_Proportion::GetDeservedResource() {
  return deserved_resource_;
}
Resources_Available Queue_Proportion::GetAllocatedResource() {
  return allocated_resource_;
}
Resources_Available Queue_Proportion::GetRequestedResource() {
  return requested_resource_;
}
}
