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

#include <grpc++/grpc++.h>

#include <ctime>
#include "base/resource_status.h"
#include "base/resource_topology_node_desc.pb.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/trace_generator.h"
#include "misc/utils.h"
#include "misc/wall_time.h"
#include "platforms/sim/simulated_messaging_adapter.h"
#include "scheduling/event_driven_scheduler.h"
#include "scheduling/firmament_scheduler.grpc.pb.h"
#include "scheduling/firmament_scheduler.pb.h"
#include "scheduling/flow/flow_scheduler.h"
#include "scheduling/knowledge_base_populator.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/scheduling_delta.pb.h"
#include "scheduling/simple/simple_scheduler.h"
#include "storage/simple_object_store.h"
#include "scheduling/proportion_queue.h"
#include "base/pod_group_desc.pb.h"
#include "scheduling/firmament_scheduler_service_utils.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using firmament::scheduler::FlowScheduler;
using firmament::scheduler::ObjectStoreInterface;
using firmament::scheduler::SchedulerInterface;
using firmament::scheduler::SchedulerStats;
using firmament::scheduler::SimpleScheduler;
using firmament::scheduler::TopologyManager;
using firmament::platform::sim::SimulatedMessagingAdapter;

DECLARE_bool(gather_unscheduled_tasks);
DEFINE_string(firmament_scheduler_service_address, "127.0.0.1",
              "The address of the scheduler service");
DEFINE_string(firmament_scheduler_service_port, "9090",
              "The port of the scheduler service");
DECLARE_bool(resource_stats_update_based_on_resource_reservation);
DEFINE_string(service_scheduler, "flow", "Scheduler to use: flow | simple");
DEFINE_uint64(queue_based_scheduling_time, 100,
              "Queue Based Schedule run time");
DECLARE_bool(proportion_drf_based_scheduling);

namespace firmament {

#define DEFAULT_Q_NAME "default"
#define MIN_MEMBER_FOR_NILL_PG_JOB 1

class FirmamentSchedulerServiceImpl final : public FirmamentScheduler::Service {
 public:
  FirmamentSchedulerServiceImpl() {
    job_map_.reset(new JobMap_t);
    task_map_.reset(new TaskMap_t);
    resource_map_.reset(new ResourceMap_t);
    knowledge_base_.reset(new KnowledgeBase);
    topology_manager_.reset(new TopologyManager);
    ResourceStatus* top_level_res_status = CreateTopLevelResource();
    top_level_res_id_ =
        ResourceIDFromString(top_level_res_status->descriptor().uuid());
    sim_messaging_adapter_ = new SimulatedMessagingAdapter<BaseMessage>();
    trace_generator_ = new TraceGenerator(&wall_time_);
    queue_map_.reset(new QueueMap_t);
    firmament_scheduler_serivice_utils_ =  Firmament_Scheduler_Service_Utils::Instance();


    if (FLAGS_service_scheduler == "flow") {
      scheduler_ = new FlowScheduler(
          job_map_, resource_map_,
          top_level_res_status->mutable_topology_node(), obj_store_, task_map_,
          knowledge_base_, topology_manager_, sim_messaging_adapter_, NULL,
          top_level_res_id_, "", &wall_time_, trace_generator_, &labels_map_,
          &affinity_antiaffinity_tasks_);
      // Get cost model pointer to clear unscheduled tasks of previous
      // scheduling round and get unscheduled tasks of current scheduling round.
      cost_model_ = dynamic_cast<FlowScheduler*>(scheduler_)->cost_model();
    } else if (FLAGS_service_scheduler == "simple") {
      scheduler_ = new SimpleScheduler(
          job_map_, resource_map_,
          top_level_res_status->mutable_topology_node(), obj_store_, task_map_,
          knowledge_base_, topology_manager_, sim_messaging_adapter_, NULL,
          top_level_res_id_, "", &wall_time_, trace_generator_);
    } else {
      LOG(FATAL) << "Flag specifies unknown scheduler "
                 << FLAGS_service_scheduler;
    }

    kb_populator_ = new KnowledgeBasePopulator(knowledge_base_);
  }

  ~FirmamentSchedulerServiceImpl() {
    delete scheduler_;
    delete sim_messaging_adapter_;
    delete trace_generator_;
    delete kb_populator_;
  }

  void HandlePlacementDelta(const SchedulingDelta& delta) {
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, delta.task_id());
    CHECK_NOTNULL(td_ptr);
    td_ptr->set_start_time(wall_time_.GetCurrentTimestamp());
  }

  void HandlePreemptionDelta(const SchedulingDelta& delta) {
    // TODO(ionel): Implement!
  }

  void HandleMigrationDelta(const SchedulingDelta& delta) {
    // TODO(ionel): Implement!
  }

  // Helper function that update the knowledge base with resource stats samples
  // based on task resource request reservation. We can use this function when
  // we do not have external dynamic resource stats provider like heapster in
  // kubernetes. If add is true, then tast resource request is subtracted from
  // available machine resources. else tast resource request is added back to
  // available machine resources.
  void UpdateMachineSamplesToKnowledgeBaseStatically(
      const TaskDescriptor* td_ptr, bool add) {
    ResourceID_t res_id = ResourceIDFromString(td_ptr->scheduled_to_resource());
    ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
    ResourceStats resource_stats;
    CpuStats* cpu_stats = resource_stats.add_cpus_stats();
    bool have_sample = knowledge_base_->GetLatestStatsForMachine(
        ResourceIDFromString(rs->mutable_topology_node()->parent_id()),
        &resource_stats);
    if (have_sample) {
      if (add) {
        cpu_stats->set_cpu_allocatable(cpu_stats->cpu_allocatable() +
                                       td_ptr->resource_request().cpu_cores());
        resource_stats.set_mem_allocatable(
            resource_stats.mem_allocatable() +
            td_ptr->resource_request().ram_cap());
        // ephemeral storage
        resource_stats.set_ephemeral_storage_allocatable(
            resource_stats.ephemeral_storage_allocatable() +
            td_ptr->resource_request().ephemeral_storage());
      } else {
        cpu_stats->set_cpu_allocatable(cpu_stats->cpu_allocatable() -
                                       td_ptr->resource_request().cpu_cores());
        resource_stats.set_mem_allocatable(
            resource_stats.mem_allocatable() -
            td_ptr->resource_request().ram_cap());
        // ephemeral storage
        resource_stats.set_ephemeral_storage_allocatable(
            resource_stats.ephemeral_storage_allocatable() -
            td_ptr->resource_request().ephemeral_storage());
      }
      double cpu_utilization =
          (cpu_stats->cpu_capacity() - cpu_stats->cpu_allocatable()) /
          (double)cpu_stats->cpu_capacity();
      cpu_stats->set_cpu_utilization(cpu_utilization);
      double mem_utilization =
          (resource_stats.mem_capacity() - resource_stats.mem_allocatable()) /
          (double)resource_stats.mem_capacity();
      resource_stats.set_mem_utilization(mem_utilization);
      double ephemeral_storage_utilization =
          (resource_stats.ephemeral_storage_capacity() - resource_stats.ephemeral_storage_allocatable()) /
          (double)resource_stats.ephemeral_storage_capacity();
      resource_stats.set_ephemeral_storage_utilization(ephemeral_storage_utilization);
      knowledge_base_->AddMachineSample(resource_stats);
    }
  }

  // Update Non-Firmament related node information.
  void UpdateStatsToKnowledgeBase(ResourceStats* resource_stats,
                                    CpuStats* cpu_stats) {
    double cpu_utilization =
        (cpu_stats->cpu_capacity() - cpu_stats->cpu_allocatable()) /
        (double)cpu_stats->cpu_capacity();
    cpu_stats->set_cpu_utilization(cpu_utilization);
    double mem_utilization = (resource_stats->mem_capacity() -
        resource_stats->mem_allocatable()) /
        (double)resource_stats->mem_capacity();
    resource_stats->set_mem_utilization(mem_utilization);
    double ephemeral_storage_utilization = (resource_stats->ephemeral_storage_capacity() -
        resource_stats->ephemeral_storage_allocatable()) /
        (double)resource_stats->ephemeral_storage_capacity();
    resource_stats->set_ephemeral_storage_utilization(ephemeral_storage_utilization);
    knowledge_base_->AddMachineSample(*resource_stats);
  }

  Status AddTaskInfo (ServerContext* context, const TaskInfo* request,
                      TaskInfoResponse* response) override {
    //boost::lock_guard<boost::recursive_mutex> lock(
      //  scheduler_->scheduling_lock_);
    ResourceID_t res_id = ResourceIDFromString(request->resource_id());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL || rs_ptr->mutable_descriptor() == NULL) {
      response->set_type(TaskInfoReplyType::TASKINFO_SUBMIT_FAILED);
      return Status::OK;
    }
    ResourceStats resource_stats;
    CpuStats* cpu_stats = resource_stats.add_cpus_stats();
    bool have_sample = knowledge_base_->GetLatestStatsForMachine(
        res_id, &resource_stats);
    if (have_sample) {
      switch (request->type()) {
        case TaskInfoType::TASKINFO_ADD: {
          if (!InsertIfNotPresent(&task_resource_map_,
                                   request->task_name(), res_id)) {
            response->set_type(TaskInfoReplyType::TASKINFO_SUBMIT_FAILED);
            return Status::OK;
          }
          cpu_stats->set_cpu_allocatable(
              cpu_stats->cpu_allocatable() -
              request->cpu_utilization());
          resource_stats.set_mem_allocatable(
              resource_stats.mem_allocatable() -
              request->mem_utilization());
          resource_stats.set_ephemeral_storage_allocatable(
              resource_stats.ephemeral_storage_allocatable() -
              request->ephemeral_storage_utilization());
          knowledge_base_->UpdateResourceNonFirmamentTaskCount(res_id, true);
          UpdateStatsToKnowledgeBase(&resource_stats, cpu_stats);
          response->set_type(TaskInfoReplyType::TASKINFO_SUBMITTED_OK);
          return Status::OK;
        }
        case TaskInfoType::TASKINFO_REMOVE: {
          ResourceID_t* rid = FindOrNull(task_resource_map_,
                                         request->task_name());
          if (rid == NULL) {
            response->set_type(TaskInfoReplyType::TASKINFO_REMOVE_FAILED);
            return Status::OK;
          }
          cpu_stats->set_cpu_allocatable(
              cpu_stats->cpu_allocatable() +
              request->cpu_utilization());
          resource_stats.set_mem_allocatable(
              resource_stats.mem_allocatable() +
              request->mem_utilization());
          resource_stats.set_ephemeral_storage_allocatable(
              resource_stats.ephemeral_storage_allocatable() +
              request->ephemeral_storage_utilization());
          knowledge_base_->UpdateResourceNonFirmamentTaskCount(res_id, false);
          UpdateStatsToKnowledgeBase(&resource_stats, cpu_stats);
          response->set_type(TaskInfoReplyType::TASKINFO_REMOVED_OK);
          return Status::OK;
        }
        default:
          LOG(FATAL) << "Unsupported request type: " << request->type();
      }
    }
    return Status::OK;
  }

  Status Schedule(ServerContext* context, const ScheduleRequest* request,
                  SchedulingDeltas* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(
        scheduler_->scheduling_lock_);
    // Clear unscheduled tasks related maps and sets of previous scheduling
    // round.
    if (FLAGS_gather_unscheduled_tasks) {
      cost_model_->ClearUnscheduledTasksData();
    }

    SchedulerStats sstat;
    vector<SchedulingDelta> deltas;
    // Schedule tasks which does not have pod affinity/anti-affinity
    // requirements.
    scheduler_->ScheduleAllJobs(&sstat, &deltas);

    uint64_t total_unsched_tasks_size = 0;
    vector<uint64_t> unscheduled_batch_tasks;
    if (FLAGS_gather_unscheduled_tasks) {
      // Get unscheduled tasks of above scheduling round.
      cost_model_->GetUnscheduledTasks(&unscheduled_batch_tasks);
    }
    // [pod affinity/anti-affinity batch schedule]
    vector<TaskID_t>* unsched_batch_affinity_tasks =
                  scheduler_->ScheduleAllAffinityBatchJobs(&sstat, &deltas);
    for (auto unsched_batch_affinity_task : *unsched_batch_affinity_tasks) {
      unscheduled_batch_tasks.push_back(unsched_batch_affinity_task);
    }
    delete unsched_batch_affinity_tasks;

    // Queue schedule tasks having pod affinity/anti-affinity.
    clock_t start = clock();
    uint64_t elapsed = 0;
    unordered_set<uint64_t> unscheduled_affinity_tasks_set;
    vector<uint64_t> unscheduled_affinity_tasks;
    while (affinity_antiaffinity_tasks_.size() &&
           (elapsed < FLAGS_queue_based_scheduling_time)) {
      uint64_t task_scheduled =
          scheduler_->ScheduleAllQueueJobs(&sstat, &deltas);
      TaskID_t task_id = dynamic_cast<FlowScheduler*>(scheduler_)
                             ->GetSingleTaskTobeScheduled();
      if (FLAGS_gather_unscheduled_tasks) {
        TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
        if (td_ptr) {
          JobDescriptor* jd =
                  FindOrNull(*job_map_, JobIDFromString(td_ptr->job_id()));
          if (!(jd->is_gang_scheduling_job())) {
            if (!task_scheduled) {
              if (unscheduled_affinity_tasks_set.find(task_id) ==
                                    unscheduled_affinity_tasks_set.end()) {
                unscheduled_affinity_tasks_set.insert(task_id);
                unscheduled_affinity_tasks.push_back(task_id);
              }
            } else {
              unscheduled_affinity_tasks_set.erase(task_id);
            }
          }
        }
      }
      clock_t stop = clock();
      elapsed = (double)(stop - start) * 1000.0 / CLOCKS_PER_SEC;
    }
    scheduler_->UpdateGangSchedulingDeltas(&sstat, &deltas,
                                &unscheduled_batch_tasks,
                                &unscheduled_affinity_tasks_set,
                                &unscheduled_affinity_tasks);
    // Get unscheduled tasks of above scheduling round which tried scheduling
    // tasks having pod affinity/anti-affinity. And populate the same into
    // reply.
    if (FLAGS_gather_unscheduled_tasks) {
      auto unscheduled_batch_tasks_ret = reply->mutable_unscheduled_tasks();
      for (auto& unsched_task : unscheduled_batch_tasks) {
        uint64_t* unsched_task_ret = unscheduled_batch_tasks_ret->Add();
        *unsched_task_ret = unsched_task;
        total_unsched_tasks_size++;
      }
      cost_model_->ClearUnscheduledTasksData();
      auto unscheduled_affinity_tasks_ret = reply->mutable_unscheduled_tasks();
      for (auto& unsched_task : unscheduled_affinity_tasks) {
        if (unscheduled_affinity_tasks_set.find(unsched_task) !=
            unscheduled_affinity_tasks_set.end()) {
          uint64_t* unsched_task_ret = unscheduled_affinity_tasks_ret->Add();
          *unsched_task_ret = unsched_task;
          total_unsched_tasks_size++;
        }
      }
    }

    // Extract scheduling results.
    LOG(INFO) << "Got " << deltas.size() << " scheduling deltas";
    if (FLAGS_gather_unscheduled_tasks) {
      LOG(INFO) << "Got " << total_unsched_tasks_size << " unscheduled tasks";
    }
    for (auto& d : deltas) {
      // LOG(INFO) << "Delta: " << d.DebugString();
      SchedulingDelta* ret_delta = reply->add_deltas();
      ret_delta->CopyFrom(d);
      if (d.type() == SchedulingDelta::PLACE) {
        HandlePlacementDelta(d);
      } else if (d.type() == SchedulingDelta::PREEMPT) {
        HandlePreemptionDelta(d);
      } else if (d.type() == SchedulingDelta::MIGRATE) {
        HandleMigrationDelta(d);
      } else if (d.type() == SchedulingDelta::NOOP) {
        // We do not have to do anything.
      } else {
        LOG(FATAL) << "Encountered unsupported scheduling delta of type "
                   << to_string(d.type());
      }
    }
    return Status::OK;
  }

  // Pod affinity/anti-affinity
  void RemoveTaskFromLabelsMap(const TaskDescriptor td) {
    for (const auto& label : td.labels()) {
      unordered_map<string, vector<TaskID_t>>* label_values =
          FindOrNull(labels_map_, label.key());
      if (label_values) {
        vector<TaskID_t>* labels_map_tasks =
            FindOrNull(*label_values, label.value());
        if (labels_map_tasks) {
          vector<TaskID_t>::iterator it_pos = find(
              labels_map_tasks->begin(), labels_map_tasks->end(), td.uid());
          if (it_pos != labels_map_tasks->end()) {
            labels_map_tasks->erase(it_pos);
            if (!labels_map_tasks->size()) {
              label_values->erase(label.value());
              if (label_values->empty()) labels_map_.erase(label.key());
            }
          }
        }
      }
    }
    if (td.has_affinity() && (td.affinity().has_pod_affinity() ||
                              td.affinity().has_pod_anti_affinity())) {
      vector<TaskID_t>::iterator it =
          find(affinity_antiaffinity_tasks_.begin(),
               affinity_antiaffinity_tasks_.end(), td.uid());
      if (it != affinity_antiaffinity_tasks_.end()) {
        affinity_antiaffinity_tasks_.erase(it);
      }
    }
  }

  Status TaskCompleted(ServerContext* context, const TaskUID* tid_ptr,
                       TaskCompletedResponse* reply) override {
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, tid_ptr->task_uid());
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    if (FLAGS_resource_stats_update_based_on_resource_reservation) {
      if (!td_ptr->scheduled_to_resource().empty()) {
        UpdateMachineSamplesToKnowledgeBaseStatically(td_ptr, true);
      }
    }
    JobID_t job_id = JobIDFromString(td_ptr->job_id());
    JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
    if (jd_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_JOB_NOT_FOUND);
      return Status::OK;
    }

    if (FLAGS_proportion_drf_based_scheduling) {
      unordered_map<JobID_t, string, boost::hash<JobID_t>>* job_id_to_pod_group =
          firmament_scheduler_serivice_utils_->GetJobIdToPodGroupMap();
      string* pod_group_name =
          FindOrNull(*job_id_to_pod_group, JobIDFromString(td_ptr->job_id()));
      // deduct the resources of task from the allocated of PG
      // 1.get the pod group name 2. then get allocated value and deduct value
      // insert the element with pod group name and resource allocated by it.
      unordered_map<string, Resource_Allocated>* pgToResourceAllocted =
          firmament_scheduler_serivice_utils_->GetPGToResourceAllocated();
      Resource_Allocated* allocted_resource =
          FindOrNull(*pgToResourceAllocted, *pod_group_name);
      float cpu_cores = td_ptr->resource_request().cpu_cores();
      uInt64_t ram_cap = td_ptr->resource_request().ram_cap();
      uInt64_t ephemeral_storage = td_ptr->resource_request().ephemeral_storage();

      if (allocted_resource != NULL) {
        allocted_resource->cpu_resource -= cpu_cores;
        allocted_resource->memory_resource -= ram_cap;
        allocted_resource->ephemeral_resource -= ephemeral_storage;

      } else {// do nothing
      }
      // Queue proportion need to be updated
      // 1.once task completed we need to deduct allocated proportion value of Q
      // 2
      unordered_map<string, string>* pod_group_to_queue_map_ptr =
          firmament_scheduler_serivice_utils_->GetPodGroupToQueue();

      string* queue_name = FindOrNull(*pod_group_to_queue_map_ptr, *pod_group_name);

      if (queue_name != NULL) {
        unordered_map<string, Queue_Proportion>* queue_map_Proportion_ptr =
            firmament_scheduler_serivice_utils_->GetQueueMapToProportion();
        Queue_Proportion* qProportion =
            FindOrNull(*queue_map_Proportion_ptr, *queue_name);
        if (qProportion != NULL) {
          qProportion->DeductAllocatedResource(cpu_cores, ram_cap,
                                               ephemeral_storage);
        } else { /* can assert here*/
        }
      } else { /*No Queue name do we need to put it into default Queue or assert*/
      }
    }
    td_ptr->set_finish_time(wall_time_.GetCurrentTimestamp());
    RemoveTaskFromLabelsMap(*td_ptr);
    TaskFinalReport report;
    scheduler_->HandleTaskCompletion(td_ptr, &report);
    kb_populator_->PopulateTaskFinalReport(*td_ptr, &report);
    scheduler_->HandleTaskFinalReport(report, td_ptr);
    // Check if it was the last task of the job.
    uint64_t* num_incomplete_tasks =
        FindOrNull(job_num_incomplete_tasks_, job_id);
    CHECK_NOTNULL(num_incomplete_tasks);
    CHECK_GE(*num_incomplete_tasks, 1);
    (*num_incomplete_tasks)--;
    if (*num_incomplete_tasks == 0) {
      scheduler_->HandleJobCompletion(job_id);

      if (FLAGS_proportion_drf_based_scheduling) {
        // remove the item from job_id to pod group map
        unordered_map<JobID_t, string, boost::hash<JobID_t>>* job_id_to_pod_group =
            firmament_scheduler_serivice_utils_->GetJobIdToPodGroupMap();
        job_id_to_pod_group->erase(job_id);
      }
    }
    reply->set_type(TaskReplyType::TASK_COMPLETED_OK);
    return Status::OK;
  }

  Status TaskFailed(ServerContext* context, const TaskUID* tid_ptr,
                    TaskFailedResponse* reply) override {
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, tid_ptr->task_uid());
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }

    if(FLAGS_proportion_drf_based_scheduling) {
      // 3 things to do
      // 1.reomve allocated resource from PG if task is running
      // 2. remove allocaed resource from Queue if it is running else
      // 3. remove requested resource from Queue

      // doing step 1.reomve allocated resource from PG if task is running
      // remove the task from the task to podgroup
      unordered_map<JobID_t, string, boost::hash<JobID_t>>* job_id_to_pod_group =
          firmament_scheduler_serivice_utils_->GetJobIdToPodGroupMap();
      string* pod_group_name =
          FindOrNull(*job_id_to_pod_group, JobIDFromString(td_ptr->job_id()));
      // task_to_pod_group->erase(tid_ptr->task_uid());
      // deduct the resources of task from the allocated if task state is  running.
      // 1.get the pod group name(PG level) 2. then get allocated value and
      // deduct(PG level)

      TaskDescriptor_TaskState task_state = td_ptr->state();

      float cpu_cores = td_ptr->resource_request().cpu_cores();
      uInt64_t ram_cap = td_ptr->resource_request().ram_cap();
      uInt64_t ephemeral_storage = td_ptr->resource_request().ephemeral_storage();

      unordered_map<string, Queue_Proportion>* queue_map_Proportion_ptr =
          firmament_scheduler_serivice_utils_->GetQueueMapToProportion();

      unordered_map<string, string>* pod_group_to_queue_map_ptr =
          firmament_scheduler_serivice_utils_->GetPodGroupToQueue();

      string* queue_name = FindOrNull(*pod_group_to_queue_map_ptr, *pod_group_name);

      Queue_Proportion* qProportion = NULL;
      if (queue_name != NULL) {
        qProportion = FindOrNull(*queue_map_Proportion_ptr, *queue_name);
      } else { /*No Queue name do we need to put it into default Queue or assert*/
      }

      if (task_state == TaskDescriptor::RUNNING) {
        // insert the element with pod group name and resource allocated by it.
        unordered_map<string, Resource_Allocated>* pgToResourceAllocted =
            firmament_scheduler_serivice_utils_->GetPGToResourceAllocated();
        Resource_Allocated* allocted_resource =
            FindOrNull(*pgToResourceAllocted, *pod_group_name);

        if (allocted_resource != NULL) {
          allocted_resource->cpu_resource -= cpu_cores;
          allocted_resource->memory_resource -= ram_cap;
          allocted_resource->ephemeral_resource -= ephemeral_storage;

        } else {
          // do nothing
        }
        // step 2. remove allocaed resource from Queue if it is running else

        if (qProportion != NULL) {
          qProportion->DeductAllocatedResource(cpu_cores, ram_cap,
                                               ephemeral_storage);
        } else {
          // do we need to assert?
        }

      } else if (task_state == TaskDescriptor::CREATED ||
                 task_state == TaskDescriptor::RUNNABLE ||
                 task_state == TaskDescriptor::BLOCKING ||
                 task_state == TaskDescriptor::ASSIGNED) {
        // step 3. remove requested resource from Queue
        qProportion->DeductRequestedResource(cpu_cores, ram_cap, ephemeral_storage);
      }
    }
    if (FLAGS_resource_stats_update_based_on_resource_reservation) {
      if (!td_ptr->scheduled_to_resource().empty()) {
        UpdateMachineSamplesToKnowledgeBaseStatically(td_ptr, true);
      }
    }
    scheduler_->HandleTaskFailure(td_ptr);
    reply->set_type(TaskReplyType::TASK_FAILED_OK);
    return Status::OK;
  }

  Status TaskRemoved(ServerContext* context, const TaskUID* tid_ptr,
                     TaskRemovedResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(
        scheduler_->scheduling_lock_);
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, tid_ptr->task_uid());
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    RemoveTaskFromLabelsMap(*td_ptr);
    if (FLAGS_resource_stats_update_based_on_resource_reservation) {
      if (!(td_ptr->scheduled_to_resource().empty()) &&
          (td_ptr->state() != TaskDescriptor::COMPLETED) &&
          (td_ptr->state() != TaskDescriptor::FAILED)) {
        UpdateMachineSamplesToKnowledgeBaseStatically(td_ptr, true);
      }
    }

    scheduler_->HandleTaskRemoval(td_ptr);
    JobID_t job_id = JobIDFromString(td_ptr->job_id());
    JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
    CHECK_NOTNULL(jd_ptr);

    if (FLAGS_proportion_drf_based_scheduling) {
      // TBD **** flag starts here
      // 3 steps here
      // 1. Queue proportion on requested resource need to be updated
      // 2. updtate the pod group
      // 3. Queue proportion on allocated resource

      unordered_map<string, Queue_Proportion>* queue_map_Proportion_ptr =
          firmament_scheduler_serivice_utils_->GetQueueMapToProportion();

      unordered_map<string, string>* pod_group_to_queue_map_ptr =
          firmament_scheduler_serivice_utils_->GetPodGroupToQueue();

      string* queue_name =
          FindOrNull(*pod_group_to_queue_map_ptr, jd_ptr->pod_group_name());

      Queue_Proportion* qProportion = NULL;
      if (queue_name != NULL) {
        qProportion = FindOrNull(*queue_map_Proportion_ptr, *queue_name);
      }

      float cpu_cores = td_ptr->resource_request().cpu_cores();
      uInt64_t ram_cap = td_ptr->resource_request().ram_cap();
      uInt64_t ephemeral_storage = td_ptr->resource_request().ephemeral_storage();

      // step 1: deduct the requested resources
      if (td_ptr->state() == TaskDescriptor::CREATED ||
          td_ptr->state() == TaskDescriptor::RUNNABLE ||
          td_ptr->state() == TaskDescriptor::BLOCKING ||
          td_ptr->state() == TaskDescriptor::ASSIGNED) {
        // update the Queue proportion here
        if (qProportion != NULL) {
          qProportion->DeductRequestedResource(cpu_cores, ram_cap,
                                               ephemeral_storage);
        }

      } else if (td_ptr->state() == TaskDescriptor::RUNNING) {
        // step 2: deduct the requested resources
        // remove the task from the task to podgroup
        unordered_map<JobID_t, string, boost::hash<JobID_t>>* job_id_to_pod_group =
            firmament_scheduler_serivice_utils_->GetJobIdToPodGroupMap();
        string* pod_group_name = FindOrNull(*job_id_to_pod_group, job_id);
        // task_to_pod_group->erase(tid_ptr->task_uid());
        // deduct the resources of task from the allocated
        // 1.get the pod group name 2. then get allocated value and deduct value

        // insert the element with pod group name and resource allocated by it.
        unordered_map<string, Resource_Allocated>* pgToResourceAllocted =
            firmament_scheduler_serivice_utils_->GetPGToResourceAllocated();
        Resource_Allocated* allocted_resource =
            FindOrNull(*pgToResourceAllocted, *pod_group_name);

        if (allocted_resource != NULL) {
          allocted_resource->cpu_resource -= cpu_cores;
          allocted_resource->memory_resource -= ram_cap;
          allocted_resource->ephemeral_resource -= ephemeral_storage;

        } else { /* *** TBD we can assert here */
        }

        // step 3 : Queue allocated resource
        if (qProportion != NULL) {
          qProportion->DeductAllocatedResource(cpu_cores, ram_cap,
                                               ephemeral_storage);
        } else { /* we can assert here or grce fully handle*/
        }

      }// deduct from allocated
    }

    // Don't remove the root task so that tasks can still be appended to
    // the job. We only remove the root task when the job completes.
    if (td_ptr != jd_ptr->mutable_root_task()) {
      task_map_->erase(td_ptr->uid());
    }
    uint64_t* num_tasks_to_remove =
        FindOrNull(job_num_tasks_to_remove_, job_id);
    CHECK_NOTNULL(num_tasks_to_remove);
    (*num_tasks_to_remove)--;
    if (*num_tasks_to_remove == 0) {
      uint64_t* num_incomplete_tasks =
          FindOrNull(job_num_incomplete_tasks_, job_id);
      if (*num_incomplete_tasks > 0) {
        scheduler_->HandleJobRemoval(job_id);
      }
      // Delete the job because we removed its last task.
      task_map_->erase(jd_ptr->root_task().uid());
      job_map_->erase(job_id);
      job_num_incomplete_tasks_.erase(job_id);
      job_num_tasks_to_remove_.erase(job_id);
    }
    reply->set_type(TaskReplyType::TASK_REMOVED_OK);
    return Status::OK;
  }

  // Pod affinity/anti-affinity
  // Adding labels of task to the labels_map_
  void AddTaskToLabelsMap(const TaskDescriptor& td) {
    TaskID_t task_id = td.uid();
    for (const auto& label : td.labels()) {
      unordered_map<string, vector<TaskID_t>>* label_values =
          FindOrNull(labels_map_, label.key());
      if (!label_values) {
        vector<TaskID_t> tasks;
        tasks.push_back(task_id);
        unordered_map<string, vector<TaskID_t>> values;
        CHECK(InsertIfNotPresent(&values, label.value(), tasks));
        CHECK(InsertIfNotPresent(&labels_map_, label.key(), values));
      } else {
        vector<TaskID_t>* labels_map_tasks =
            FindOrNull(*label_values, label.value());
        if (!labels_map_tasks) {
          vector<TaskID_t> value_tasks;
          value_tasks.push_back(task_id);
          CHECK(
              InsertIfNotPresent(&(*label_values), label.value(), value_tasks));
        } else {
          labels_map_tasks->push_back(task_id);
        }
      }
    }
    if (td.has_affinity() && (td.affinity().has_pod_affinity() ||
                              td.affinity().has_pod_anti_affinity())) {
      unordered_set<TaskID_t>* no_conflict_tasks =
                               scheduler_->GetNoConflictTasksSet();
      JobID_t job_id = JobIDFromString(td.job_id());
      JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
      if (no_conflict_tasks->find(jd_ptr->root_task().uid()) == no_conflict_tasks->end()) {
        affinity_antiaffinity_tasks_.push_back(task_id);
      }
    }
  }

  Status TaskSubmitted(ServerContext* context,
                       const TaskDescription* task_desc_ptr,
                       TaskSubmittedResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(
        scheduler_->scheduling_lock_);

    LOG(INFO)<<"*** pod_group_name = "<<task_desc_ptr->job_descriptor().pod_group_name()<<endl;
    LOG(INFO)<<"*** uuid = "<<task_desc_ptr->job_descriptor().uuid()<<endl;
    TaskID_t task_id = task_desc_ptr->task_descriptor().uid();
    if (FindPtrOrNull(*task_map_, task_id)) {
      reply->set_type(TaskReplyType::TASK_ALREADY_SUBMITTED);
      return Status::OK;
    }
    if (task_desc_ptr->task_descriptor().state() != TaskDescriptor::CREATED) {
      reply->set_type(TaskReplyType::TASK_STATE_NOT_CREATED);
      return Status::OK;
    }
    JobID_t job_id = JobIDFromString(task_desc_ptr->task_descriptor().job_id());
    JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);

    if (jd_ptr == NULL) {

      CHECK(InsertIfNotPresent(job_map_.get(), job_id,
                               task_desc_ptr->job_descriptor()));
      jd_ptr = FindOrNull(*job_map_, job_id);
      TaskDescriptor* root_td_ptr = jd_ptr->mutable_root_task();
      // Task that comes first is made as root task of the job.
      // Root task that was set in poseidon is ignored.
      root_td_ptr->CopyFrom(task_desc_ptr->task_descriptor());
      CHECK(
          InsertIfNotPresent(task_map_.get(), root_td_ptr->uid(), root_td_ptr));
      root_td_ptr->set_submit_time(wall_time_.GetCurrentTimestamp());
      CHECK(InsertIfNotPresent(&job_num_incomplete_tasks_, job_id, 0));
      CHECK(InsertIfNotPresent(&job_num_tasks_to_remove_, job_id, 0));
    } else {
      TaskDescriptor* td_ptr = jd_ptr->mutable_root_task()->add_spawned();
      td_ptr->CopyFrom(task_desc_ptr->task_descriptor());
      CHECK(InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr));
      td_ptr->set_submit_time(wall_time_.GetCurrentTimestamp());
      scheduler_->UpdateSpawnedToRootTaskMap(td_ptr);
    }
    uint64_t* num_incomplete_tasks =
        FindOrNull(job_num_incomplete_tasks_, job_id);
    CHECK_NOTNULL(num_incomplete_tasks);
    if (*num_incomplete_tasks == 0) {
      scheduler_->AddJob(jd_ptr);

      if (FLAGS_proportion_drf_based_scheduling) {
        string pod_group_name(jd_ptr->pod_group_name());
        if(pod_group_name == string("")) {
          LOG(INFO) <<"****Pod group name is Empty"<<endl;
          //pod group name is empty soadd a pod group to the queue which is the
          //uid of the jobdesc and minmember as 1.
          pod_group_name = task_desc_ptr->task_descriptor().job_id();
          PodGroupAdded(pod_group_name);
        }
        unordered_map<JobID_t, string,
            boost::hash<JobID_t>>* job_id_to_pod_group =
            firmament_scheduler_serivice_utils_->GetJobIdToPodGroupMap();
        InsertIfNotPresent(job_id_to_pod_group, job_id, pod_group_name);
        LOG(INFO) << "PodGroup name from Job = "
                  << jd_ptr->pod_group_name() << endl;
      }
    }
    AddTaskToLabelsMap(task_desc_ptr->task_descriptor());
    (*num_incomplete_tasks)++;
    uint64_t* num_tasks_to_remove =
        FindOrNull(job_num_tasks_to_remove_, job_id);
    (*num_tasks_to_remove)++;

    if(FLAGS_proportion_drf_based_scheduling) {
      AddRequestedResource(task_desc_ptr->task_descriptor(), *jd_ptr);
    }

    reply->set_type(TaskReplyType::TASK_SUBMITTED_OK);
    return Status::OK;
  }

  Status TaskUpdated(ServerContext* context,
                     const TaskDescription* task_desc_ptr,
                     TaskUpdatedResponse* reply) override {
    TaskID_t task_id = task_desc_ptr->task_descriptor().uid();
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    // The scheduler will notice that the task's properties (e.g.,
    // resource requirements, labels) are different and react accordingly.
    const TaskDescriptor& updated_td = task_desc_ptr->task_descriptor();
    td_ptr->mutable_resource_request()->CopyFrom(updated_td.resource_request());
    td_ptr->set_priority(updated_td.priority());
    td_ptr->clear_labels();
    for (const auto& label : updated_td.labels()) {
      Label* label_ptr = td_ptr->add_labels();
      label_ptr->CopyFrom(label);
    }
    td_ptr->clear_label_selectors();
    for (const auto& label_selector : updated_td.label_selectors()) {
      LabelSelector* label_sel_ptr = td_ptr->add_label_selectors();
      label_sel_ptr->CopyFrom(label_selector);
    }
    // XXX(ionel): We may want to add support for other field updates as well.
    reply->set_type(TaskReplyType::TASK_UPDATED_OK);
    return Status::OK;
  }

  bool CheckResourceDoesntExist(const ResourceDescriptor& rd) {
    ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, ResourceIDFromString(rd.uuid()));
    return rs_ptr == NULL;
  }

  void AddResource(ResourceTopologyNodeDescriptor* rtnd_ptr) {
    ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
    ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
    ResourceStatus* rs_ptr =
        new ResourceStatus(rd_ptr, rtnd_ptr, rd_ptr->friendly_name(), 0);
    CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));
  }

  Status NodeAdded(ServerContext* context,
                   const ResourceTopologyNodeDescriptor* submitted_rtnd_ptr,
                   NodeAddedResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(
        scheduler_->scheduling_lock_);
    LOG(INFO)<<" Node Added"<<endl;
    bool doesnt_exist = DFSTraverseResourceProtobufTreeWhileTrue(
        *submitted_rtnd_ptr,
        boost::bind(&FirmamentSchedulerServiceImpl::CheckResourceDoesntExist,
                    this, _1));
    if (!doesnt_exist) {
      reply->set_type(NodeReplyType::NODE_ALREADY_EXISTS);
      return Status::OK;
    }
    ResourceStatus* root_rs_ptr =
        FindPtrOrNull(*resource_map_, top_level_res_id_);
    CHECK_NOTNULL(root_rs_ptr);
    ResourceTopologyNodeDescriptor* rtnd_ptr =
        root_rs_ptr->mutable_topology_node()->add_children();
    rtnd_ptr->CopyFrom(*submitted_rtnd_ptr);
    rtnd_ptr->set_parent_id(to_string(top_level_res_id_));
    DFSTraverseResourceProtobufTreeReturnRTND(
        rtnd_ptr,
        boost::bind(&FirmamentSchedulerServiceImpl::AddResource, this, _1));
    // TODO(ionel): we use a hack here -- we pass simulated=true to
    // avoid Firmament instantiating an actual executor for this resource.
    // Instead, we rely on the no-op SimulatedExecutor. We should change
    // it such that Firmament does not mandatorily create an executor.
    scheduler_->RegisterResource(rtnd_ptr, false, true);
    reply->set_type(NodeReplyType::NODE_ADDED_OK);

    if (FLAGS_resource_stats_update_based_on_resource_reservation) {
      // Add Node initial status simulation
      ResourceStats resource_stats;
      ResourceID_t res_id =
          ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
      ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
      if (rs_ptr == NULL || rs_ptr->mutable_descriptor() == NULL) {
        reply->set_type(NodeReplyType::NODE_NOT_FOUND);
        return Status::OK;
      }
      resource_stats.set_resource_id(rtnd_ptr->resource_desc().uuid());
      resource_stats.set_timestamp(0);

      //add all the node capcity and allocatable to the aggregate
      ResourceStatsAggregate resAgg;

      CpuStats* cpu_stats = resource_stats.add_cpus_stats();
      // As some of the resources is utilized by system pods, so initializing
      // utilization to 10%.
      int64_t cpu_cores =
        rtnd_ptr->resource_desc().resource_capacity().cpu_cores();
      cpu_stats->set_cpu_capacity(cpu_cores);
      resAgg.resource_capcity.cpu_resource = cpu_cores;

      cpu_cores =  rtnd_ptr->resource_desc().available_resources().cpu_cores();
      cpu_stats->set_cpu_allocatable(cpu_cores);
      resAgg.resource_allocatable.cpu_resource = cpu_cores;

      double cpu_utilization =
          (cpu_stats->cpu_capacity() - cpu_stats->cpu_allocatable()) /
          (double)cpu_stats->cpu_capacity();
      cpu_stats->set_cpu_utilization(cpu_utilization);
      int64_t ram = rtnd_ptr->resource_desc().resource_capacity().ram_cap();
      resource_stats.set_mem_capacity(ram);
      resAgg.resource_capcity.memory_resource = ram;

      ram = rtnd_ptr->resource_desc().available_resources().ram_cap();
      resource_stats.set_mem_allocatable(ram);
      resAgg.resource_allocatable.memory_resource = ram;

      double mem_utilization =
          (resource_stats.mem_capacity() - resource_stats.mem_allocatable()) /
          (double)resource_stats.mem_capacity();
      resource_stats.set_mem_utilization(mem_utilization);
      resource_stats.set_disk_bw(0);
      resource_stats.set_net_rx_bw(0);
      resource_stats.set_net_tx_bw(0);
      // ephemeral storage
      int64_t ephemeral_storage =
          rtnd_ptr->resource_desc().resource_capacity().ephemeral_storage();
      resource_stats.set_ephemeral_storage_capacity(ephemeral_storage);
      resAgg.resource_capcity.ephemeral_resource = ephemeral_storage;

      ephemeral_storage =
          rtnd_ptr->resource_desc().available_resources().ephemeral_storage();
      resource_stats.set_ephemeral_storage_allocatable(ephemeral_storage);
      resAgg.resource_allocatable.ephemeral_resource = ephemeral_storage;

      double ephemeral_storage_utilization =
          (resource_stats.ephemeral_storage_capacity() -
           resource_stats.ephemeral_storage_allocatable()) /
          (double)resource_stats.ephemeral_storage_capacity();
      resource_stats.set_ephemeral_storage_utilization(
                                             ephemeral_storage_utilization);

      //add all capacity allocatable to the aggregate.
      //so that no need to loop through all the nodes to get the total cpacity
      //and allocatable
      knowledge_base_->AddToResourceStatsAgg(resAgg);

      knowledge_base_->AddMachineSample(resource_stats);
    }
    return Status::OK;
  }

  Status NodeFailed(ServerContext* context, const ResourceUID* rid_ptr,
                    NodeFailedResponse* reply) override {
    ResourceID_t res_id = ResourceIDFromString(rid_ptr->resource_uid());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL) {
      reply->set_type(NodeReplyType::NODE_NOT_FOUND);
      return Status::OK;
    }

    //deduct all the node capcity and allocatable from the aggregate
    ResourceStatsAggregate resAgg;
    ResourceStats resource_stats;
    knowledge_base_->GetLatestStatsForMachine(res_id, &resource_stats);
    //if it is multicore then we need to go through each of the elements
    CpuStats cpu_stats = resource_stats.cpus_stats(0);
    resAgg.resource_capcity.cpu_resource = cpu_stats.cpu_capacity();
    resAgg.resource_capcity.memory_resource = resource_stats.mem_capacity();
    resAgg.resource_capcity.ephemeral_resource
        = resource_stats.ephemeral_storage_capacity();

    resAgg.resource_allocatable.cpu_resource =  cpu_stats.cpu_allocatable();
    resAgg.resource_allocatable.memory_resource =
        resource_stats.mem_allocatable();
    resAgg.resource_allocatable.ephemeral_resource =
        resource_stats.ephemeral_storage_allocatable();
    //deduct the capacity and allocated resources from ResourceStatusAgg
    knowledge_base_->DeductFromResourceStatsAgg(resAgg);

    scheduler_->DeregisterResource(rs_ptr->mutable_topology_node());
    reply->set_type(NodeReplyType::NODE_FAILED_OK);
    return Status::OK;
  }

  Status NodeRemoved(ServerContext* context, const ResourceUID* rid_ptr,
                     NodeRemovedResponse* reply) override {
    ResourceID_t res_id = ResourceIDFromString(rid_ptr->resource_uid());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL) {
      reply->set_type(NodeReplyType::NODE_NOT_FOUND);
      return Status::OK;
    }

    ResourceStatsAggregate resAgg;
    ResourceStats resource_stats;// = knowledge_base_->GetStatsForMachine(res_id);
    knowledge_base_->GetLatestStatsForMachine(res_id, &resource_stats);

    CpuStats cpu_stats = resource_stats.cpus_stats(0);
    resAgg.resource_capcity.cpu_resource = cpu_stats.cpu_capacity();
    resAgg.resource_capcity.memory_resource = resource_stats.mem_capacity();
    resAgg.resource_capcity.ephemeral_resource = resource_stats.ephemeral_storage_capacity();

    resAgg.resource_allocatable.cpu_resource =  cpu_stats.cpu_allocatable();
    resAgg.resource_allocatable.memory_resource = resource_stats.mem_allocatable();
    resAgg.resource_allocatable.ephemeral_resource= resource_stats.ephemeral_storage_allocatable();
    //deduct the capacity and allocated resources from ResourceStatusAgg
    knowledge_base_->DeductFromResourceStatsAgg(resAgg);

    scheduler_->DeregisterResource(rs_ptr->mutable_topology_node());
    reply->set_type(NodeReplyType::NODE_REMOVED_OK);
    return Status::OK;
  }

  Status NodeUpdated(ServerContext* context,
                     const ResourceTopologyNodeDescriptor* updated_rtnd_ptr,
                     NodeUpdatedResponse* reply) override {
    ResourceID_t res_id =
        ResourceIDFromString(updated_rtnd_ptr->resource_desc().uuid());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL) {
      reply->set_type(NodeReplyType::NODE_NOT_FOUND);
      return Status::OK;
    }
    DFSTraverseResourceProtobufTreesReturnRTNDs(
        rs_ptr->mutable_topology_node(), *updated_rtnd_ptr,
        boost::bind(&FirmamentSchedulerServiceImpl::UpdateNodeLabels, this, _1,
                    _2));
    DFSTraverseResourceProtobufTreesReturnRTNDs(
        rs_ptr->mutable_topology_node(), *updated_rtnd_ptr,
        boost::bind(&FirmamentSchedulerServiceImpl::UpdateNodeTaints, this, _1,
                    _2));
    // TODO(ionel): Support other types of node updates.
    reply->set_type(NodeReplyType::NODE_UPDATED_OK);
    return Status::OK;
  }

  void UpdateNodeLabels(ResourceTopologyNodeDescriptor* old_rtnd_ptr,
                        const ResourceTopologyNodeDescriptor& new_rtnd_ptr) {
    ResourceDescriptor* old_rd_ptr = old_rtnd_ptr->mutable_resource_desc();
    const ResourceDescriptor& new_rd = new_rtnd_ptr.resource_desc();
    old_rd_ptr->clear_labels();
    for (const auto& label : new_rd.labels()) {
      Label* label_ptr = old_rd_ptr->add_labels();
      label_ptr->CopyFrom(label);
    }
  }

  void UpdateNodeTaints(ResourceTopologyNodeDescriptor* old_rtnd_ptr,
                        const ResourceTopologyNodeDescriptor& new_rtnd_ptr) {
    ResourceDescriptor* old_rd_ptr = old_rtnd_ptr->mutable_resource_desc();
    const ResourceDescriptor& new_rd = new_rtnd_ptr.resource_desc();
    old_rd_ptr->clear_taints();
    for (const auto& taint : new_rd.taints()) {
      Taint* taint_ptr = old_rd_ptr->add_taints();
      taint_ptr->CopyFrom(taint);
    }
  }

  Status AddTaskStats(ServerContext* context, const TaskStats* task_stats,
                      TaskStatsResponse* reply) override {
    TaskID_t task_id = task_stats->task_id();
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    knowledge_base_->AddTaskStatsSample(*task_stats);
    return Status::OK;
  }

  Status AddNodeStats(ServerContext* context,
                      const ResourceStats* resource_stats,
                      ResourceStatsResponse* reply) override {
    ResourceID_t res_id = ResourceIDFromString(resource_stats->resource_id());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL || rs_ptr->mutable_descriptor() == NULL) {
      reply->set_type(NodeReplyType::NODE_NOT_FOUND);
      return Status::OK;
    }
    knowledge_base_->AddMachineSample(*resource_stats);
    return Status::OK;
  }

  Status Check(ServerContext* context, const HealthCheckRequest* health_service,
               HealthCheckResponse* reply) override {
    if (health_service->grpc_service().empty()) {
      reply->set_status(ServingStatus::SERVING);
    }
    return Status::OK;
  }

  /**
   *Add Queue as and when it is created and update
   *deserved proportion
   **/
  Status QueueAdded(ServerContext* context,
                    const QueueDescriptor* queue_desc_ptr,
                    QueueAddedResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(scheduler_->scheduling_lock_);

    if (queue_desc_ptr != NULL) {
      string queue_name(queue_desc_ptr->name());
      if(queue_name == string("")) {
      bool exist = InsertIfNotPresent(queue_map_.get(), queue_name,
                                      *queue_desc_ptr);
      if (!exist) {
        LOG(INFO) << "Queue :   "<<queue_name<<"already added"<<endl ;
        reply->set_type(QUEUE_ALREADY_ADDED);
      } else {
        // insert the queue name into the map with dummy weight '0'
        InsertIfNotPresent(&queue_map_ratio_, queue_name, 0);
        // calculate ratio based on the number of Queue present
        CalculatePropotionRatio();

        unordered_map<string, Queue_Proportion>* queue_map_proportion_ptr =
            firmament_scheduler_serivice_utils_->GetQueueMapToProportion();

        Queue_Proportion qProportion;
        InsertIfNotPresent(queue_map_proportion_ptr, queue_name,
                           qProportion);
        LOG(INFO) << "Queue added name = "<< queue_name
             << "Queue weight =" << queue_desc_ptr->weight() << endl;
        // deseved propotion need to be updated as it is available
        // as and when Queue is added
        UpdateDeservedProportion();
        reply->set_type(QUEUE_ADDED_OK);
      }
    } else {
      LOG(INFO) << "Queue name is empty :   "<<endl ;
      reply->set_type(QUEUE_INVALID_OK);
    }
  } else {
    LOG(INFO) << " Queue descriptor NULL :   "<<endl ;
    reply->set_type(QUEUE_FAILED_OK);
  }

    return Status::OK;
  }

  /**
   *Remove Queue
   **/
  Status QueueRemoved(ServerContext* context,
                      const QueueDescriptor* queue_desc_ptr,
                      QueueRemoveResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(scheduler_->scheduling_lock_);
    if(queue_desc_ptr != NULL) {
      string queue_name(queue_desc_ptr->name());
      if(queue_name != string("")) {
        if(queue_map_.get()->erase(queue_name)) {
          queue_map_ratio_.erase(queue_name);
          unordered_map<string, Queue_Proportion>* queue_map_proportion_ptr =
              firmament_scheduler_serivice_utils_->GetQueueMapToProportion();
          queue_map_proportion_ptr->erase(queue_name);
          LOG(INFO) << " Queue removed = " <<queue_name<< endl;
          reply->set_type(QUEUE_REMOVED_OK);
        } else {
          LOG(INFO) << " Queue not present =" <<endl;
          reply->set_type(QUEUE_NOT_FOUND);
        }
      }else {
        LOG(INFO) << " Queue name is empty " <<endl;
        reply->set_type(QUEUE_INVALID_OK);
      }
    } else {
      LOG(INFO) << " QueueDescriptor is NULL " <<endl;
      reply->set_type(QUEUE_INVALID_OK);
    }
    return Status::OK;
  }

/**
 *Update Queue
 **/
  Status QueueUpdated(ServerContext* context,
                      const QueueDescriptor* queue_desc_ptr,
                      QueueUpdateResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(scheduler_->scheduling_lock_);
    if(queue_desc_ptr) {
      string queue_name(queue_desc_ptr->name());
      if(queue_name != string("")) {
        QueueDescriptor* queue_desc = FindOrNull(*queue_map_.get() , queue_name);
        if(queue_desc) {
          queue_desc->set_weight(queue_desc_ptr->weight());
          CalculatePropotionRatio();
          UpdateDeservedProportion();
          LOG(INFO) << "Queue updated = " <<queue_name<< endl;
          reply->set_type(QUEUE_UPDATED_OK);
        } else {
          LOG(INFO) << "Queue not present" << endl;
          reply->set_type(QUEUE_NOT_FOUND);
        }
      } else {
        LOG(INFO) << "Queue name is empty" << endl;
        reply->set_type(QUEUE_INVALID_OK);
      }
    } else {
      LOG(INFO) << "QueueDescriptor is NULL " << endl;
      reply->set_type(QUEUE_FAILED_OK);
    }
    return Status::OK;
  }

  /**
   *Calculate the new ratio for all queue/s in the system
   **/
  void CalculatePropotionRatio() {
    int32_t aggWeight = 0;

    for (thread_safe::map<string, QueueDescriptor>::iterator it =
         queue_map_.get()->begin(); it != queue_map_.get()->end(); ++it) {
      aggWeight += it->second.weight();
    }
    LOG(INFO)<<"total weight of all queue ="<< aggWeight<<endl;

    for (thread_safe::map<string, QueueDescriptor>::iterator it =
         queue_map_.get()->begin(); it != queue_map_.get()->end(); ++it) {

      //auto it1 = queue_map_ratio_.begin();
      double* proportion = FindOrNull(queue_map_ratio_ , it->first);
      if(proportion) {
      // can put check for .end() //but its not necessary
      *proportion = ((double)it->second.weight() / aggWeight);
      LOG(INFO)<<"proportion ratio of "
               << it->second.name()<<"= "<<*proportion<<endl;
      } else {
      LOG(INFO)<<"Proportion calculation failed"<<endl;
      }
    }
  }

  /**
   *
   */
  void UpdateDeservedProportion() {
    unordered_map<string, Queue_Proportion>* queue_map_proportion_ptr =
        firmament_scheduler_serivice_utils_->GetQueueMapToProportion();

    for (unordered_map<string, double>::iterator it = queue_map_ratio_.begin();
         it != queue_map_ratio_.end(); ++it) {

      LOG(INFO)<<"Qname ="<<it->first<<endl;
      Queue_Proportion* itProportion = FindOrNull(*queue_map_proportion_ptr,
                                                  it->first);
      if(itProportion) {
        ResourceStatsAggregate resAgg = knowledge_base_->GetResourceStatsAgg();
        //*** TBD simplify this
        itProportion->SetDeservedResource(
            resAgg.resource_allocatable.cpu_resource * it->second,
            resAgg.resource_allocatable.memory_resource * it->second,
            resAgg.resource_allocatable.ephemeral_resource * it->second);
      } else {
      cout<<"itProportion == NULL"<<endl;
      }
    }
  }

 /**
  *Add Podgroup
  */
  Status PodGroupAdded(ServerContext* context,
                       const PodGroupDescriptor* pod_group_desc_ptr,
                       PodGroupAddedResponse* reply) override {

    boost::lock_guard<boost::recursive_mutex> lock(
    scheduler_->scheduling_lock_);
    if(pod_group_desc_ptr){
      auto pod_group_map_ptr =
          firmament_scheduler_serivice_utils_->GetPodGroupMap();
      bool exist = InsertIfNotPresent(pod_group_map_ptr,
                                      pod_group_desc_ptr->name(),
                                      *pod_group_desc_ptr);
      if(!exist) {
        reply->set_type(PODGROUP_ALREADY_ADDED);
        LOG(INFO)<<"pg already added "<<endl;
      } else {
        unordered_map<string, string>* pod_group_to_queue_map_ptr =
        firmament_scheduler_serivice_utils_->GetPodGroupToQueue();
        string queue_name(pod_group_desc_ptr->queuename());
        LOG(INFO)<<"PodGroup Added name = "<<pod_group_desc_ptr->name()
            <<"Queue name in Pg ="<< queue_name<<endl;
        if(queue_name == string("")) {
          queue_name = string(DEFAULT_Q_NAME);
          }
        InsertIfNotPresent(pod_group_to_queue_map_ptr,
        pod_group_desc_ptr->name(),queue_name);

        //insert the element with pod group name and resource allocated by it.
        unordered_map<string, Resource_Allocated> * pgToResourceAllocted =
        firmament_scheduler_serivice_utils_->GetPGToResourceAllocated();
        Resource_Allocated resAllocated ;
        InsertIfNotPresent(pgToResourceAllocted, pod_group_desc_ptr->name(),
                           resAllocated);
        //LOG(INFO)<<"PodGroup Added name = "<<pod_group_desc_ptr->name()
        //    <<"Queue name in Pg ="<< pod_group_desc_ptr->queuename()<<endl;
        reply->set_type(PODGROUP_ADDED_OK);
    }
  } else {
    reply->set_type(PODGROUP_INVALID_OK);
  }
  return Status::OK;
  }

 /**
  *Podgroup Removed
  */
Status PodGroupRemoved(ServerContext* context,
                       const PodGroupDescriptor* pod_group_desc_ptr,
                       PodGroupRemoveResponse* reply) override {

  boost::lock_guard<boost::recursive_mutex> lock(
  scheduler_->scheduling_lock_);
  string pod_group_name(pod_group_desc_ptr->name());
  if(pod_group_name != string("")) {
    LOG(INFO) << "Pod Group removed = " <<pod_group_name <<endl;
    auto pod_group_map_ptr =
        firmament_scheduler_serivice_utils_->GetPodGroupMap();
    auto pod_group_desc_ptr =
        FindOrNull(*pod_group_map_ptr, pod_group_name);
    if(pod_group_desc_ptr) {
      pod_group_map_ptr->erase(pod_group_name);
      auto pod_group_to_queue_map_ptr =
          firmament_scheduler_serivice_utils_->GetPodGroupToQueue();
      pod_group_to_queue_map_ptr->erase(pod_group_name);
      auto pg_to_resource_allocated_map_ptr =
          firmament_scheduler_serivice_utils_->GetPGToResourceAllocated();
      pg_to_resource_allocated_map_ptr->erase(pod_group_name);
      auto job_id_to_pod_group_map_ptr =
            firmament_scheduler_serivice_utils_->GetJobIdToPodGroupMap();
      for(auto it = job_id_to_pod_group_map_ptr->begin();
          it != job_id_to_pod_group_map_ptr->end(); ++it) {
        if(it->second == pod_group_name) {
          job_id_to_pod_group_map_ptr->erase(it->first);
        }
        auto q_to_ordered_pg_list_map_ptr =
              firmament_scheduler_serivice_utils_->GetQtoOrderedPgListMap();
        for(auto it = q_to_ordered_pg_list_map_ptr->begin();
            it != q_to_ordered_pg_list_map_ptr->end(); ++it) {
          if(*it->second.begin() == pod_group_name) {
            q_to_ordered_pg_list_map_ptr->erase(it->first);
          }
        }
      }
    }
    reply->set_type(PODGROUP_REMOVED_OK);
  } else {
    LOG(INFO) << "pod group name is empty"<<endl;
    reply->set_type(PODGROUP_INVALID_OK);
  }
  return Status::OK;
}

/**
 *PodGroup Updated
 */
  Status PodGroupUpdated(ServerContext* context,
                         const PodGroupDescriptor* queue_desc_ptr,
                         PodGroupUpdateResponse* reply) override {

    boost::lock_guard<boost::recursive_mutex> lock(scheduler_->scheduling_lock_);
    LOG(INFO) << "Why should we update pod group ???" << endl;
    reply->set_type(PODGROUP_UPDATED_OK);
    return Status::OK;
  }

 /**
  *Job desc dose not have a podgroup name in which
  */
  void PodGroupAdded(string pod_group_name) {

  PodGroupDescriptor pod_group_descriptor;
  pod_group_descriptor.set_name(pod_group_name);
  pod_group_descriptor.set_queuename(DEFAULT_Q_NAME);
  pod_group_descriptor.set_minmember(MIN_MEMBER_FOR_NILL_PG_JOB);
  unordered_map<string, string>* pod_group_to_queue_map_ptr =
     firmament_scheduler_serivice_utils_->GetPodGroupToQueue();
  auto pod_group_map_ptr =
      firmament_scheduler_serivice_utils_->GetPodGroupMap();
  InsertIfNotPresent(pod_group_map_ptr, pod_group_name, pod_group_descriptor);
  InsertIfNotPresent(pod_group_to_queue_map_ptr, pod_group_name,
                     DEFAULT_Q_NAME);
  }


 private:
  SchedulerInterface* scheduler_;
  SimulatedMessagingAdapter<BaseMessage>* sim_messaging_adapter_;
  TraceGenerator* trace_generator_;
  WallTime wall_time_;
  // Data structures thare are populated by the scheduler. The service should
  // never have to directly insert values in these data structures.
  boost::shared_ptr<JobMap_t> job_map_;
  boost::shared_ptr<KnowledgeBase> knowledge_base_;
  boost::shared_ptr<ObjectStoreInterface> obj_store_;
  boost::shared_ptr<TaskMap_t> task_map_;
  boost::shared_ptr<TopologyManager> topology_manager_;
  // Data structures that we populate in the scheduler service.
  boost::shared_ptr<ResourceMap_t> resource_map_;
  ResourceID_t top_level_res_id_;
  // Mapping from JobID_t to number of incomplete job tasks.
  unordered_map<JobID_t, uint64_t, boost::hash<boost::uuids::uuid>>
      job_num_incomplete_tasks_;
  // Mapping from JobID_t to number of job tasks left to be removed.
  unordered_map<JobID_t, uint64_t, boost::hash<boost::uuids::uuid>>
      job_num_tasks_to_remove_;
  KnowledgeBasePopulator* kb_populator_;
  // Pod affinity/anti-affinity
  unordered_map<string, unordered_map<string, vector<TaskID_t>>> labels_map_;
  vector<TaskID_t> affinity_antiaffinity_tasks_;
  unordered_map<string, ResourceID_t> task_resource_map_;
  //multi tenant support throug Queue
  boost::shared_ptr<QueueMap_t>queue_map_;
  unordered_map<string, double> queue_map_ratio_;
  unordered_map<string, Queue_Proportion> queue_map_Proportion_;

  Firmament_Scheduler_Service_Utils* firmament_scheduler_serivice_utils_;

  ResourceStatus* CreateTopLevelResource() {
    ResourceID_t res_id = GenerateResourceID();
    ResourceTopologyNodeDescriptor* rtnd_ptr =
        new ResourceTopologyNodeDescriptor();
    // Set up the RD
    ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
    rd_ptr->set_uuid(to_string(res_id));
    rd_ptr->set_type(ResourceDescriptor::RESOURCE_COORDINATOR);
    // Need to maintain a ResourceStatus for the resource map
    ResourceStatus* rs_ptr =
        new ResourceStatus(rd_ptr, rtnd_ptr, "root_resource", 0);
    // Insert into resource map
    CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));
    return rs_ptr;
  }

  /**
   * Add requested resources to the Queue
   */
  void AddRequestedResource(const TaskDescriptor& task_desc,
                            const JobDescriptor& job_desc) {
  //*** TBD do we need this lools like it is not useful
    unordered_map<string, Queue_Proportion>* queue_map_Proportion_ptr =
        firmament_scheduler_serivice_utils_->GetQueueMapToProportion();

    unordered_map<string, string>* pod_group_to_queue_map_ptr =
        firmament_scheduler_serivice_utils_->GetPodGroupToQueue();

    // AddRequestedResource(task_desc_ptr->task_descriptor(), *jd_ptr);
    // update the Queue proportion here
    string* queue_name =
        FindOrNull(*pod_group_to_queue_map_ptr, job_desc.pod_group_name());
    if (queue_name && (*queue_name != string(""))) {
      Queue_Proportion* qProportion =
          FindOrNull(*queue_map_Proportion_ptr, *queue_name);
      if(qProportion){
        qProportion->AddRequestedResource(
            task_desc.resource_request().cpu_cores(),
            task_desc.resource_request().ram_cap(),
            task_desc.resource_request().ephemeral_storage());
      } else {
        LOG(INFO)<< "* propotion is null"<<endl;
      }
   } else {
     // do we need to put it into default Queue?
   }
  }

  boost::recursive_mutex task_submission_lock_;
  boost::recursive_mutex node_addition_lock_;
  CostModelInterface* cost_model_;
};
}  // namespace firmament

int main(int argc, char* argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  firmament::common::InitFirmament(argc, argv);
  std::string server_address(FLAGS_firmament_scheduler_service_address + ":" +
                             FLAGS_firmament_scheduler_service_port);
  LOG(INFO) << "Firmament scheduler starting ...";
  firmament::FirmamentSchedulerServiceImpl scheduler;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&scheduler);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  LOG(INFO) << "Firmament scheduler listening on " << server_address;
  server->Wait();
  return 0;
}
