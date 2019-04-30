#include<iostream>
#include "scheduling/proportion_queue.h"

using namespace std;
namespace firmament {

  Queue_Proportion::Queue_Proportion() {}
  Queue_Proportion::~Queue_Proportion() {}
  void Queue_Proportion::SetDeservedResource(float cpuRes, uInt64_t memoryRes,
                                             uInt64_t epheRes) {
    cout<<"*** Deserved Res Cpu = "<<cpuRes<<"memory = "<< memoryRes
        <<"epheRes ="<<epheRes<<endl;
    deserved_resource_.SetResources(cpuRes, memoryRes, epheRes);
  }

  void Queue_Proportion::SetAllocatedResource(float cpuRes, uInt64_t memoryRes,
                                              uInt64_t epheRes) {
    allocated_resource_.SetResources(cpuRes, memoryRes, epheRes);
  }

  void Queue_Proportion::SetRequestedResource(float cpuRes, uInt64_t memoryRes,
                                              uInt64_t epheRes) {
    requested_resource_.SetResources(cpuRes, memoryRes, epheRes);
  }

  void Queue_Proportion::AddAllocatedResource(float cpuRes, uInt64_t memoryRes,
                                              uInt64_t epheRes) {
    allocated_resource_.AddResources(cpuRes, memoryRes, epheRes);
  }

  void Queue_Proportion::AddRequestedResource(float cpuRes,
                                              uInt64_t memoryRes,
                                              uInt64_t epheRes) {
    requested_resource_.AddResources(cpuRes, memoryRes, epheRes);
  }

  void Queue_Proportion::DeductAllocatedResource(float cpuRes,
                                                 uInt64_t memoryRes,
                                                 uInt64_t epheRes) {
    allocated_resource_.DeductResources(cpuRes, memoryRes, epheRes);
  }
  void Queue_Proportion::DeductRequestedResource(float cpuRes,
                                                 uInt64_t memoryRes,
                                                 uInt64_t epheRes) {
    requested_resource_.DeductResources(cpuRes, memoryRes, epheRes);
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

}//end of name space firmament
