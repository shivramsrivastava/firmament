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

// Tests for cpu memory cost model.

#include "scheduling/flow/cpu_cost_model.h"
#include <gtest/gtest.h>
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

DECLARE_uint64(max_multi_arcs_for_cpu);
DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

// The fixture for testing the CpuCostModel.
class CpuCostModelTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  CpuCostModelTest() {
    // You can do set-up work for each test here.
    resource_map_.reset(new ResourceMap_t);
    task_map_.reset(new TaskMap_t);
    knowledge_base_.reset(new KnowledgeBase);
    cost_model =
        new CpuCostModel(resource_map_, task_map_, knowledge_base_, &labels_map_);
  }

  virtual ~CpuCostModelTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
    delete cost_model;
  }

  // TODO(shivramsrivastava): Copied from CreateTask() func from
  // flow_grap_manager_test. Need to write common unit test utility module.
  TaskDescriptor* CreateTask(JobDescriptor* jd_ptr, uint64_t job_id_seed) {
    JobID_t job_id = GenerateJobID(job_id_seed);
    jd_ptr->set_uuid(to_string(job_id));
    jd_ptr->set_name(to_string(job_id));
    TaskDescriptor* td_ptr = jd_ptr->mutable_root_task();
    td_ptr->set_uid(GenerateRootTaskID(*jd_ptr));
    td_ptr->set_job_id(jd_ptr->uuid());
    return td_ptr;
  }

  CpuCostModel* cost_model;
  boost::shared_ptr<ResourceMap_t> resource_map_;
  boost::shared_ptr<TaskMap_t> task_map_;
  boost::shared_ptr<KnowledgeBase> knowledge_base_;
  unordered_map<string, unordered_map<string, vector<TaskID_t>>> labels_map_;
};

TEST_F(CpuCostModelTest, AddMachine) {
  ResourceID_t res_id = GenerateResourceID("test");
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(res_id));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  //***Suresh*** this is required to check the number of possible arcs to be created
  //ele test will fail
  rd_ptr->set_max_pods(FLAGS_max_multi_arcs_for_cpu);
  
  cost_model->AddMachine(&rtnd);
  EXPECT_EQ(1UL, cost_model->ecs_for_machines_.size());
  unordered_map<ResourceID_t, vector<EquivClass_t>,
                boost::hash<ResourceID_t>>::iterator it =
      cost_model->ecs_for_machines_.find(res_id);
  EXPECT_NE(cost_model->ecs_for_machines_.end(), it);
  EXPECT_EQ(FLAGS_max_multi_arcs_for_cpu, it->second.size());
  // Remove test machine.
  cost_model->RemoveMachine(res_id);
}

TEST_F(CpuCostModelTest, AddTask) {
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
  TaskID_t task_id = td_ptr->uid();
  td_ptr->mutable_resource_request()->set_cpu_cores(10.0);
  cost_model->AddTask(task_id);
  unordered_map<TaskID_t, CpuMemResVector_t>::iterator it =
      cost_model->task_resource_requirement_.find(task_id);
  EXPECT_NE(cost_model->task_resource_requirement_.end(), it);
  EXPECT_FLOAT_EQ(10.0, it->second.cpu_cores_);
  EXPECT_EQ(1UL, cost_model->task_resource_requirement_.size());
  // Remove test task.
  cost_model->RemoveTask(task_id);
  cost_model->task_map_.get()->erase(task_id);
}

TEST_F(CpuCostModelTest, EquivClassToEquivClass) {

  // Create Task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
  InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
  TaskID_t task_id = td_ptr->uid();
  td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
  td_ptr->mutable_resource_request()->set_ram_cap(1000);
  cost_model->AddTask(task_id);
  // Get task equivalence classes.
  vector<EquivClass_t>* equiv_classes =
      cost_model->GetTaskEquivClasses(task_id);
  // Create machine and its equivalence classes.
  ResourceID_t res_id = GenerateResourceID("test1");
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(res_id));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  //to have to ec0 and ec1 having pods
  rd_ptr->set_max_pods(2);
  ResourceVector* resource_capacity = rd_ptr->mutable_resource_capacity();
  ResourceVector* available_resources = rd_ptr->mutable_available_resources();
  resource_capacity->set_cpu_cores(1000.0);
  resource_capacity->set_ram_cap(32000);
  available_resources->set_cpu_cores(500.0);
  available_resources->set_ram_cap(16000);
  ResourceStatus resource_status =
      ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
  ResourceStatus* rs_ptr = &resource_status;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));

  cost_model->AddMachine(&rtnd); 

//  vector<EquivClass_t>::iterator it1 = equiv_classes->begin();
  	
  //cost_model->CalculatePrioritiesCost()
  //MinMaxScores_t priority_scores_list;
  //InsertIfNotPresent(&ec_to_max_min_priority_scores, ec,
    //                           priority_scores_list);

	unordered_map<ResourceID_t, PriorityScoresList_t,
                        boost::hash<boost::uuids::uuid>>
              node_to_priority_scores_map;   
  InsertIfNotPresent(&(cost_model->ec_to_node_priority_scores), (*equiv_classes)[0],node_to_priority_scores_map);

  unordered_map<ResourceID_t, PriorityScoresList_t,
                boost::hash<boost::uuids::uuid>>* nodes_priority_scores_ptr1 = FindOrNull(cost_model->ec_to_node_priority_scores, (*equiv_classes)[0]);
  
   CHECK_NOTNULL(nodes_priority_scores_ptr1);
	PriorityScoresList_t priority_scores_list;

	//ResourceID_t res_id = ResourceIDFromString(rd.uuid());
	PriorityScoresList_t* priority_scores_struct_ptr =
	FindOrNull(*nodes_priority_scores_ptr1, res_id);
	if (!priority_scores_struct_ptr) {

		InsertIfNotPresent(nodes_priority_scores_ptr1, res_id, priority_scores_list);
		priority_scores_struct_ptr = FindOrNull(*nodes_priority_scores_ptr1, res_id);
	}


	MinMaxScores_t* max_min_priority_scores =
		FindOrNull(cost_model->ec_to_max_min_priority_scores, (*equiv_classes)[0]);
	if (!max_min_priority_scores) {
		MinMaxScores_t priority_scores_list;
		InsertIfNotPresent(&cost_model->ec_to_max_min_priority_scores, (*equiv_classes)[0],priority_scores_list);
		max_min_priority_scores = FindOrNull(cost_model->ec_to_max_min_priority_scores, (*equiv_classes)[0]);
	}
	 

  InsertIfNotPresent(&(cost_model->ec_to_node_priority_scores), (*equiv_classes)[1],node_to_priority_scores_map);
  
  unordered_map<ResourceID_t, PriorityScoresList_t,
                boost::hash<boost::uuids::uuid>>* nodes_priority_scores_ptr2 = FindOrNull(cost_model->ec_to_node_priority_scores, (*equiv_classes)[1]);

  CHECK_NOTNULL(nodes_priority_scores_ptr2);
  //ResourceID_t res_id = ResourceIDFromString(rd.uuid());
  PriorityScoresList_t* priority_scores_struct_ptr2 =
  FindOrNull(*nodes_priority_scores_ptr2, res_id);
  if (!priority_scores_struct_ptr2) {
	  PriorityScoresList_t priority_scores_list2;
	  InsertIfNotPresent(nodes_priority_scores_ptr2, res_id, priority_scores_list);
	  priority_scores_struct_ptr = FindOrNull(*nodes_priority_scores_ptr2, res_id);
  }

  
  unordered_map<ResourceID_t, vector<EquivClass_t>,
                boost::hash<ResourceID_t>>::iterator it =
      cost_model->ecs_for_machines_.find(res_id);
  EXPECT_EQ(res_id, it->first);

  vector<EquivClass_t> machine_equiv_classes = it->second;
  // Calculate cost of arc between main EC and first machine EC.
  ArcDescriptor arc_cost1 = cost_model->EquivClassToEquivClass(
      (*equiv_classes)[0], machine_equiv_classes[0]);
  // Calculate cost of arc between main EC and second machine EC.
  ArcDescriptor arc_cost2 = cost_model->EquivClassToEquivClass(
      (*equiv_classes)[0], machine_equiv_classes[1]);   
  EXPECT_EQ(2500, arc_cost1.cost_);
  EXPECT_EQ(2525, arc_cost2.cost_);
  // Cost of arc between main EC and first machine EC should be less than
  // cost of arc between main EC and second machine EC.
  EXPECT_LT(arc_cost1.cost_, arc_cost2.cost_);
  // Clean up.
  // Remove test task.
  cost_model->RemoveTask(task_id);
  cost_model->task_map_.get()->find(task_id);
  delete equiv_classes;
  // Remove test machine.
  cost_model->RemoveMachine(res_id);
  cost_model->resource_map_.get()->erase(res_id);
  
}

TEST_F(CpuCostModelTest, GetEquivClassToEquivClassesArcs) {
  // Create Task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 44);
  InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
  TaskID_t task_id = td_ptr->uid();
  td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
  td_ptr->mutable_resource_request()->set_ram_cap(1000);
  cost_model->AddTask(task_id);
  // Get task equivalence classes.
  vector<EquivClass_t>* equiv_classes =
      cost_model->GetTaskEquivClasses(task_id);
  // Create machine1 and its equivalence classes.
  ResourceID_t res_id1 = GenerateResourceID("Machine1");
  ResourceTopologyNodeDescriptor rtnd1;
  ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
  rd_ptr1->set_friendly_name("Machine1");
  rd_ptr1->set_uuid(to_string(res_id1));
  rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  rd_ptr1->set_max_pods(1);
  ResourceVector* resource_capacity1 = rd_ptr1->mutable_resource_capacity();
  ResourceVector* available_resources1 = rd_ptr1->mutable_available_resources();
  
  resource_capacity1->set_cpu_cores(1000.0);
  resource_capacity1->set_ram_cap(32000);
  resource_capacity1->set_ephemeral_storage(1000);
  	
  available_resources1->set_cpu_cores(500.0);
  available_resources1->set_ram_cap(16000);
  available_resources1->set_ephemeral_storage(500);
  
  ResourceStatus resource_status1 =
      ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
  ResourceStatus* rs_ptr1 = &resource_status1;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
  cost_model->AddMachine(&rtnd1);
  // Create machine2 and its equivalence classes.
  ResourceID_t res_id2 = GenerateResourceID("Machine2");
  ResourceTopologyNodeDescriptor rtnd2;
  ResourceDescriptor* rd_ptr2 = rtnd2.mutable_resource_desc();
  rd_ptr2->set_friendly_name("Machine2");
  rd_ptr2->set_uuid(to_string(res_id2));
  rd_ptr2->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  rd_ptr2->set_max_pods(1);
  ResourceVector* resource_capacity2 = rd_ptr2->mutable_resource_capacity();
  ResourceVector* available_resources2 = rd_ptr2->mutable_available_resources();
  resource_capacity2->set_cpu_cores(1000.0);
  resource_capacity2->set_ram_cap(32000);
  resource_capacity2->set_ephemeral_storage(1000);
  
  available_resources2->set_cpu_cores(500.0);
  available_resources2->set_ram_cap(16000);
  available_resources2->set_ephemeral_storage(500);
	
  ResourceStatus resource_status2 =
      ResourceStatus(rd_ptr2, &rtnd2, rd_ptr2->friendly_name(), 0);
  ResourceStatus* rs_ptr2 = &resource_status2;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id2, rs_ptr2));
  cost_model->AddMachine(&rtnd2);
  
  vector<EquivClass_t>* equiv_to_equiv_arcs =
      cost_model->GetEquivClassToEquivClassesArcs((*equiv_classes)[0]);
  // Since machine1 & machine2 can fit 1 tasks each, we expect no. of
  // EquivClassToEquivClassesArcs to be 2.
  EXPECT_EQ(2U, equiv_to_equiv_arcs->size());
  // Clean up.
  // Remove test task.
  cost_model->RemoveTask(task_id);
  cost_model->task_map_.get()->find(task_id);
  delete equiv_classes;
  // Remove test machine1.
  cost_model->RemoveMachine(res_id1);
  cost_model->resource_map_.get()->erase(res_id1);
  // Remove test machine1.
  cost_model->RemoveMachine(res_id2);
  cost_model->resource_map_.get()->erase(res_id2);
  delete equiv_to_equiv_arcs;
}

TEST_F(CpuCostModelTest, GatherStats) {  
  // Create machine Machine1.
  ResourceID_t res_id1 = GenerateResourceID("Machine1");
  ResourceTopologyNodeDescriptor rtnd1;
  ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
  rd_ptr1->set_friendly_name("Machine1");
  rd_ptr1->set_uuid(to_string(res_id1));
  rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceStatus resource_status1 =
      ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
  ResourceStatus* rs_ptr1 = &resource_status1;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
  // Create PU node PU1.
  ResourceTopologyNodeDescriptor* pu_rtnd =
      rs_ptr1->mutable_topology_node()->add_children();
  
  ResourceID_t res_id2 = GenerateResourceID("PU1");
  ResourceDescriptor* rd_ptr2 = pu_rtnd->mutable_resource_desc();
  rd_ptr2->set_friendly_name("Machine1_PU #0");
  rd_ptr2->set_uuid(to_string(res_id2));
  rd_ptr2->set_type(ResourceDescriptor::RESOURCE_PU);
  rd_ptr2->add_current_running_tasks(1);
  rd_ptr2->add_current_running_tasks(2);
  pu_rtnd->set_parent_id(to_string(res_id1));
  ResourceStatus resource_status2 =
      ResourceStatus(rd_ptr2, pu_rtnd, rd_ptr2->friendly_name(), 0);
  ResourceStatus* rs_ptr2 = &resource_status2;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id2, rs_ptr2));
  // Add the test machine sample of machine to knowledge base.
  ResourceStats machine_test_stats;
  machine_test_stats.set_resource_id(to_string(res_id1));
  machine_test_stats.add_cpus_stats();
  CpuStats* pu_test_stats = machine_test_stats.mutable_cpus_stats(0);
  pu_test_stats->set_cpu_utilization(0.5);
  pu_test_stats->set_cpu_capacity(1000.0);
  machine_test_stats.set_mem_utilization(0.5);
  machine_test_stats.set_mem_capacity(32000);
  rd_ptr1->set_max_pods(2);
  cost_model->knowledge_base_->AddMachineSample(machine_test_stats);
  // Create flow graph nodes.
  FlowGraphNode* machine_node = new FlowGraphNode(1);
  machine_node->type_ = FlowNodeType::MACHINE;
  machine_node->rd_ptr_ = rd_ptr1;
  machine_node->resource_id_ = res_id1;
  FlowGraphNode* pu_node = new FlowGraphNode(2);
  pu_node->type_ = FlowNodeType::PU;
  pu_node->rd_ptr_ = rd_ptr2;
  pu_node->resource_id_ = res_id2;
  FlowGraphNode* sink_node = new FlowGraphNode(-1);
  sink_node->type_ = FlowNodeType::SINK;
  // Test GatherStats from sink to PU.
  cost_model->GatherStats(pu_node, sink_node);
  // Verifying number of slots, running tasks on PU.
  EXPECT_EQ(2U, rd_ptr2->num_running_tasks_below());
  //max pad is set as 2 this should comu out as num_slots_below
  EXPECT_EQ(2U, rd_ptr2->num_slots_below());
  // Test GatherStats from PU to Machine.
  cost_model->GatherStats(machine_node, pu_node);
  // Verifying gathered available cpu and memory for machine.
  EXPECT_FLOAT_EQ(500.0, rd_ptr1->available_resources().cpu_cores());
  EXPECT_EQ(16000U, rd_ptr1->available_resources().ram_cap());
  // Clean up.
  delete machine_node;
  delete pu_node;
  delete sink_node;
  cost_model->resource_map_.get()->erase(res_id1);
  cost_model->resource_map_.get()->erase(res_id2);
}

TEST_F(CpuCostModelTest, GetOutgoingEquivClassPrefArcs) {
  // Create machine1 and its equivalence classes.
  ResourceID_t res_id1 = GenerateResourceID("Machine1");
  ResourceTopologyNodeDescriptor rtnd1;
  ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
  rd_ptr1->set_friendly_name("Machine1");
  rd_ptr1->set_uuid(to_string(res_id1));
  rd_ptr1->set_max_pods(1);
  rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceStatus resource_status =
      ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
  ResourceStatus* rs_ptr1 = &resource_status;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
  cost_model->AddMachine(&rtnd1);
  unordered_map<ResourceID_t, vector<EquivClass_t>,
                boost::hash<ResourceID_t>>::iterator it =
      cost_model->ecs_for_machines_.find(res_id1);
  EXPECT_EQ(res_id1, it->first);
  vector<EquivClass_t> machine_equiv_classes = it->second;
  vector<ResourceID_t>* equiv_class_outgoing_arcs =
  cost_model->GetOutgoingEquivClassPrefArcs(machine_equiv_classes[0]);
  // We expect only 1 arc from machine equivalence class to machine.
  EXPECT_EQ(1U, equiv_class_outgoing_arcs->size());
  // Clean up.
  // Remove test machine1.
  cost_model->RemoveMachine(res_id1);
  cost_model->resource_map_.get()->erase(res_id1);
  delete equiv_class_outgoing_arcs;
}

TEST_F(CpuCostModelTest, GetTaskEquivClasses) {
  // Create Task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 45);
  InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
  TaskID_t task_id = td_ptr->uid();
  td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
  td_ptr->mutable_resource_request()->set_ram_cap(1000);
  cost_model->AddTask(task_id);
  // Get task equivalence classes.
  vector<EquivClass_t>* equiv_classes =
      cost_model->GetTaskEquivClasses(task_id);
  // We expect only 1 equivalence class for corresponding to task resource
  // request.
  EXPECT_EQ(1U, equiv_classes->size());
  // Clean up.
  // Remove test task.
  cost_model->RemoveTask(task_id);
  cost_model->task_map_.get()->find(task_id);
  delete equiv_classes;
}

TEST_F(CpuCostModelTest, MachineResIDForResource) {
  // Create machine1 and its equivalence classes.
  ResourceID_t res_id1 = GenerateResourceID("Machine1");
  ResourceTopologyNodeDescriptor rtnd1;
  ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
  rd_ptr1->set_friendly_name("Machine1");
  rd_ptr1->set_uuid(to_string(res_id1));
  rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceStatus resource_status1 =
      ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
  ResourceStatus* rs_ptr1 = &resource_status1;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
  cost_model->AddMachine(&rtnd1);
  // Create PU node.
  ResourceTopologyNodeDescriptor* pu_rtnd =
      rs_ptr1->mutable_topology_node()->add_children();
  ResourceID_t res_id2 = GenerateResourceID("PU1");
  ResourceDescriptor* rd_ptr2 = pu_rtnd->mutable_resource_desc();
  rd_ptr2->set_friendly_name("PU1");
  rd_ptr2->set_uuid(to_string(res_id2));
  rd_ptr2->set_type(ResourceDescriptor::RESOURCE_PU);
  pu_rtnd->set_parent_id(to_string(res_id1));
  ResourceStatus resource_status2 =
      ResourceStatus(rd_ptr2, pu_rtnd, rd_ptr2->friendly_name(), 0);
  ResourceStatus* rs_ptr2 = &resource_status2;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id2, rs_ptr2));
  ResourceID_t machine_res_id = cost_model->MachineResIDForResource(res_id2);
  EXPECT_EQ(machine_res_id, res_id1);
  // Clean up.
  // Remove test machine1.
  cost_model->RemoveMachine(res_id1);
  cost_model->resource_map_.get()->erase(res_id1);
  // Remove test pu1.
  // Not removing PU since its not added as machine.
  cost_model->resource_map_.get()->erase(res_id2);
}

TEST_F(CpuCostModelTest, PrepareStats) {
	// Create Task.
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);
	cost_model->AddTask(task_id);
	// Get task equivalence classes.
	vector<EquivClass_t>* equiv_classes =
	  cost_model->GetTaskEquivClasses(task_id);
	// Create machine and its equivalence classes.
	ResourceID_t res_id = GenerateResourceID("test1");
	ResourceTopologyNodeDescriptor rtnd;
	ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
	rd_ptr->set_uuid(to_string(res_id));
	rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	rd_ptr->add_current_running_tasks(1);
	rd_ptr->add_current_running_tasks(2);
	//to have to ec0 and ec1 having pods
	rd_ptr->set_max_pods(2);
	ResourceVector* resource_capacity = rd_ptr->mutable_resource_capacity();
	ResourceVector* available_resources = rd_ptr->mutable_available_resources();
	resource_capacity->set_cpu_cores(1000.0);
	resource_capacity->set_ram_cap(32000);
	available_resources->set_cpu_cores(500.0);
	available_resources->set_ram_cap(16000);
	
	ResourceStatus resource_status =
	  ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
	ResourceStatus* rs_ptr = &resource_status;
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));

	cost_model->AddMachine(&rtnd);

	//adding priority scores 
	unordered_map<ResourceID_t, PriorityScoresList_t,
	boost::hash<boost::uuids::uuid>>
	node_to_priority_scores_map;   
	InsertIfNotPresent(&(cost_model->ec_to_node_priority_scores), (*equiv_classes)[0],node_to_priority_scores_map);

	unordered_map<ResourceID_t, PriorityScoresList_t,
	boost::hash<boost::uuids::uuid>>* nodes_priority_scores_ptr1 = FindOrNull(cost_model->ec_to_node_priority_scores, (*equiv_classes)[0]);

	CHECK_NOTNULL(nodes_priority_scores_ptr1);
	PriorityScoresList_t priority_scores_list;

	//ResourceID_t res_id = ResourceIDFromString(rd.uuid());
	PriorityScoresList_t* priority_scores_struct_ptr =
	FindOrNull(*nodes_priority_scores_ptr1, res_id);
	if (!priority_scores_struct_ptr) {

		InsertIfNotPresent(nodes_priority_scores_ptr1, res_id, priority_scores_list);
		priority_scores_struct_ptr = FindOrNull(*nodes_priority_scores_ptr1, res_id);
	}


	MinMaxScores_t* max_min_priority_scores =
	FindOrNull(cost_model->ec_to_max_min_priority_scores, (*equiv_classes)[0]);
	if (!max_min_priority_scores) {
		MinMaxScores_t priority_scores_list;
		InsertIfNotPresent(&cost_model->ec_to_max_min_priority_scores, (*equiv_classes)[0],priority_scores_list);
		max_min_priority_scores = FindOrNull(cost_model->ec_to_max_min_priority_scores, (*equiv_classes)[0]);
	}


	InsertIfNotPresent(&(cost_model->ec_to_node_priority_scores), (*equiv_classes)[1],node_to_priority_scores_map);

	unordered_map<ResourceID_t, PriorityScoresList_t,
	boost::hash<boost::uuids::uuid>>* nodes_priority_scores_ptr2 = FindOrNull(cost_model->ec_to_node_priority_scores, (*equiv_classes)[1]);

	CHECK_NOTNULL(nodes_priority_scores_ptr2);
	//ResourceID_t res_id = ResourceIDFromString(rd.uuid());
	PriorityScoresList_t* priority_scores_struct_ptr2 =
	FindOrNull(*nodes_priority_scores_ptr2, res_id);
	if (!priority_scores_struct_ptr2) {
		PriorityScoresList_t priority_scores_list2;
		InsertIfNotPresent(nodes_priority_scores_ptr2, res_id, priority_scores_list);
		priority_scores_struct_ptr = FindOrNull(*nodes_priority_scores_ptr2, res_id);
	}

	//add to flow graph node and lets remove all the values.
	// Add the test machine sample of machine to knowledge base.
	ResourceStats machine_test_stats;
	machine_test_stats.set_resource_id(to_string(res_id));
	machine_test_stats.add_cpus_stats();
	CpuStats* pu_test_stats = machine_test_stats.mutable_cpus_stats(0);
	pu_test_stats->set_cpu_utilization(0.5);
	pu_test_stats->set_cpu_capacity(1000.0);
	machine_test_stats.set_mem_utilization(0.5);
	machine_test_stats.set_mem_capacity(32000);
	rd_ptr->set_max_pods(2);
	cost_model->knowledge_base_->AddMachineSample(machine_test_stats);
	// Create flow graph nodes.
	FlowGraphNode* machine_node = new FlowGraphNode(1);
	machine_node->type_ = FlowNodeType::MACHINE;
	machine_node->rd_ptr_ = rd_ptr;
	machine_node->resource_id_ = res_id;
	
	FlowGraphNode* sink_node = new FlowGraphNode(-1);
	sink_node->type_ = FlowNodeType::SINK;
	// Test GatherStats from sink to machine
	cost_model->GatherStats(machine_node, sink_node);

	//clear all the resource related information
	cost_model->PrepareStats(machine_node);

	cost_model->GatherStats(machine_node, sink_node);

	//all the below should be 0
	EXPECT_EQ(0U, rd_ptr->num_running_tasks_below());
	EXPECT_EQ(0U, rd_ptr->num_slots_below());
	EXPECT_EQ(0U,cost_model->ec_to_node_priority_scores.size());
	EXPECT_EQ(0U,cost_model->ec_to_min_cost_.size());
	EXPECT_EQ(0U,cost_model->ec_to_best_fit_resource_.size());

	cost_model->RemoveTask(task_id);
	cost_model->task_map_.get()->find(task_id);
	delete equiv_classes;
	// Remove test machine.
	cost_model->RemoveMachine(res_id);
	cost_model->resource_map_.get()->erase(res_id);
	}

TEST_F(CpuCostModelTest, GetMachineEC) {
	ResourceID_t res_id1 = GenerateResourceID("Machine1");
	ResourceTopologyNodeDescriptor rtnd1;
	ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
	rd_ptr1->set_friendly_name("Machine1");
	rd_ptr1->set_uuid(to_string(res_id1));
	rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	rd_ptr1->set_max_pods(2);
	ResourceStatus resource_status1 =
	ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
	ResourceStatus* rs_ptr1 = &resource_status1;
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
	cost_model->AddMachine(&rtnd1);

	//this is the same method used in GetMachineEC
	//method is non testable hence locally creating it and 
	//checking the same ***for the coverage***
	uint64_t hash = HashString(rd_ptr1->friendly_name());
	boost::hash_combine(hash, 0);
	EXPECT_EQ(hash, cost_model->GetMachineEC(rd_ptr1->friendly_name(), 0));	
	}

TEST_F(CpuCostModelTest, RemoveTask){
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);
	cost_model->AddTask(task_id);
	//check the values in the cost_model
	CpuMemResVector_t* task_resource_request =
    FindOrNull(cost_model->task_resource_requirement_, task_id);
	EXPECT_EQ(20.0,task_resource_request->cpu_cores_);
	EXPECT_EQ(1000,task_resource_request->ram_cap_);

	//Remove the task from the cost_model and then 
	//check the cpu_cores and ram_cap_
	cost_model->RemoveTask(task_id);	
	CpuMemResVector_t* task_resource_request1 =
    FindOrNull(cost_model->task_resource_requirement_, task_id);
	//task_resource_request1 should be NULL to pass the test
	EXPECT_EQ(NULL, task_resource_request1);
	
	}

TEST_F(CpuCostModelTest, RemoveMachine){
	//Add machine then remove it and check the values	
	ResourceID_t res_id1 = GenerateResourceID("Machine1");
	ResourceTopologyNodeDescriptor rtnd1;
	ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
	rd_ptr1->set_friendly_name("Machine1");
	rd_ptr1->set_uuid(to_string(res_id1));
	rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	rd_ptr1->set_max_pods(2);
	ResourceStatus resource_status1 =
	ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
	ResourceStatus* rs_ptr1 = &resource_status1;
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
	cost_model->AddMachine(&rtnd1);
	
	vector<EquivClass_t>* ecs = FindOrNull(cost_model->ecs_for_machines_, res_id1);
	CHECK_NOTNULL(ecs);
	for (EquivClass_t& ec : *ecs) {
		//cost_model->GetMachineEC(rd_ptr1->friendly_name(), 0)
		CHECK_EQ( *FindOrNull(cost_model->ec_to_machine_,ec), res_id1);
		}
  
	//remove the machine 
	cost_model->RemoveMachine(res_id1);

	vector<EquivClass_t>* ecs1 = FindOrNull(cost_model->ecs_for_machines_, res_id1);
	//machine removed hence ecs1 should be NULL
	EXPECT_EQ(NULL,ecs1);  
	}

TEST_F(CpuCostModelTest,ClearUnscheduledTasksData){
	//	vector<EquivClass_t> task_ec_with_no_pref_arcs
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);
	cost_model->AddTask(task_id);
	// Get task equivalence classes.
	cost_model->task_ec_with_no_pref_arcs_ =
	*cost_model->GetTaskEquivClasses(task_id);
	//check the size before clearing
	CHECK_EQ(1, cost_model->task_ec_with_no_pref_arcs_.size());
	cost_model->task_ec_with_no_pref_arcs_set_.insert((*cost_model->GetTaskEquivClasses(task_id))[0]);
	cost_model->ClearUnscheduledTasksData();
	//check the size after clearing it should be 0
	CHECK_EQ(0, cost_model->task_ec_with_no_pref_arcs_.size());	
	CHECK_EQ(0, cost_model->task_ec_with_no_pref_arcs_set_.size());
	}

TEST_F(CpuCostModelTest,CallMethodsForCoverage){

	//Add machine then remove it and check the values	
	ResourceID_t res_id1 = GenerateResourceID("Machine1");
	ResourceTopologyNodeDescriptor rtnd1;
	ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
	rd_ptr1->set_friendly_name("Machine1");
	rd_ptr1->set_uuid(to_string(res_id1));
	rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	rd_ptr1->set_max_pods(2);
	ResourceStatus resource_status1 =
	ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
	ResourceStatus* rs_ptr1 = &resource_status1;
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
	cost_model->AddMachine(&rtnd1);
		

	// Create Task.
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);
	cost_model->AddTask(task_id);
	vector<uint64_t> unscheduled_tasks_ptr;
	cost_model->GetUnscheduledTasks(&unscheduled_tasks_ptr);
	cost_model->TaskToUnscheduledAgg(task_id);

	cost_model->TaskToResourceNode(task_id,res_id1);
	ResourceDescriptor source;
	ResourceDescriptor dest;
	cost_model->ResourceNodeToResourceNode(source,dest);

	cost_model->LeafResourceNodeToSink(res_id1);
	cost_model->TaskContinuation(task_id);
	cost_model->TaskPreemption(task_id);
	EquivClass_t ec;
	cost_model->TaskToEquivClassAggregator(task_id,ec);
	cost_model->EquivClassToResourceNode(ec, res_id1);			
	}

	TEST_F(CpuCostModelTest, CalculatePrioritiesCost){
	
	// Create Task.
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);

	//create affinity info 
	//all the pointers created will be deleted by associated setallocated classes.
	:: firmament :: Affinity* affinity = new :: firmament :: Affinity();
	//create node affinity info
	:: firmament ::NodeAffinity *nodAffinity =  new ::firmament ::NodeAffinity();
	PreferredSchedulingTerm* psTermPtr = nodAffinity->add_preferredduringschedulingignoredduringexecution();
	psTermPtr->set_weight(80);	
	//nodAffinity.	
	string str ="Test";
	string oper ="In";	
	NodeSelector* nSelctor =new NodeSelector;
	NodeSelectorTerm* nsTermForNodeSelctor = nSelctor->add_nodeselectorterms();
	NodeSelectorRequirement *nodSelctReqForNodeSelctor 
	= nsTermForNodeSelctor->add_matchexpressions();
	nodSelctReqForNodeSelctor->set_key(str);
	nodSelctReqForNodeSelctor->set_operator_(oper);
	nodSelctReqForNodeSelctor->add_values("Testing");
	
	NodeSelectorTerm* nsTerm =  new NodeSelectorTerm;
	::firmament::NodeSelectorRequirement* nsReqPtr = nsTerm->add_matchexpressions();
	nsReqPtr->set_key(str);
	nsReqPtr->set_operator_(oper);
	nsReqPtr->add_values("Testing");
	//*values = "testing";		
	psTermPtr->set_allocated_preference(nsTerm);
	nodAffinity->set_allocated_requiredduringschedulingignoredduringexecution(nSelctor);	
	affinity->set_allocated_node_affinity(nodAffinity);
	/*
	//create podAffinity and set it to affinity
	//but this is not used in the function that is called
	:: firmament ::PodAffinity* podAffinity = new  ::firmament ::PodAffinity();	
	::firmament::PodAffinityTerm* podATerm = podAffinity->add_requiredduringschedulingignoredduringexecution();
	podATerm->set_topologykey("test_topology");
	podATerm->set_namespaces(0,"test_name_space");
	
	PodLabelSelector *pLselctor = new PodLabelSelector;		
	:: firmament :: MatchLabels * matchlabels = new MatchLabels;
	matchlabels->set_key("test_key");
	matchlabels->set_value("test_value");
	pLselctor->set_allocated_matchlabels(matchlabels);		

	::firmament::LabelSelectorRequirement* lSelReq = pLselctor->add_matchexpressions();
	lSelReq->set_key("test_req_key");
	lSelReq->set_operator_("In");	
	lSelReq->set_values(0,"test_req_value");	
	podATerm->set_allocated_labelselector(pLselctor);

	
	::firmament::WeightedPodAffinityTerm* wPodATerm = podAffinity->add_preferredduringschedulingignoredduringexecution();		
	wPodATerm->set_weight(80);
	::firmament::PodAffinityTerm* podATerm1 = podAffinity->add_requiredduringschedulingignoredduringexecution();
	
	podATerm1->set_topologykey("test_topology");
	podATerm1->set_namespaces(0,"test_name_space");
	
	PodLabelSelector *pLselctor1 = new PodLabelSelector;		
	:: firmament :: MatchLabels * matchlabels1 = new MatchLabels;
	matchlabels1->set_key("test_key");
	matchlabels1->set_value("test_value");
	pLselctor1->set_allocated_matchlabels(matchlabels);		

	::firmament::LabelSelectorRequirement* lSelReq1 = pLselctor->add_matchexpressions();
	lSelReq1->set_key("test_req_key");
	lSelReq1->set_operator_("In");	
	lSelReq1->set_values(0,"test_req_value");	
	podATerm1->set_allocated_labelselector(pLselctor);

	affinity->set_allocated_pod_affinity(podAffinity);	
	*/
	td_ptr->set_allocated_affinity(affinity);	
	cost_model->AddTask(task_id);
	// Get task equivalence classes.
	vector<EquivClass_t>* equiv_classes =
	cost_model->GetTaskEquivClasses(task_id);
	// Create machine and its equivalence classes.
	ResourceID_t res_id = GenerateResourceID("test1");
	ResourceTopologyNodeDescriptor rtnd;
	ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
	rd_ptr->set_uuid(to_string(res_id));
	rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	::firmament::Label* label = rd_ptr->add_labels();
	label->set_key(str);
	label->set_value("Testing");

	rd_ptr->set_max_pods(2);
	ResourceStatus resource_status =
	ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
	ResourceStatus* rs_ptr = &resource_status;
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));	
	cost_model->AddMachine(&rtnd);	
	cost_model->CalculatePrioritiesCost((*equiv_classes)[0],*rd_ptr);
	CHECK_EQ(true, td_ptr->has_affinity());
	EXPECT_EQ(1,cost_model->ec_to_node_priority_scores.size());	

	MinMaxScores_t* max_min_priority_scores =
        FindOrNull(cost_model->ec_to_max_min_priority_scores, (*equiv_classes)[0]);

	MinMaxScore_t& min_max_node_affinity_score =
    max_min_priority_scores->node_affinity_priority;
	EXPECT_EQ(80,min_max_node_affinity_score.max_score);
	//min value is not used in nod affinity case hence it should be -1
	EXPECT_EQ(-1,min_max_node_affinity_score.min_score);		
	}
	
TEST_F(CpuCostModelTest, CalculateIntolerableTaintsCost){

	// Create Task.
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);
	//toleration and taints
	//attach the tolarations to the task
	Toleration* tolPtr = 	td_ptr->add_tolerations();
	string strValue ="Testing";
	tolPtr->set_key("Test");
	tolPtr->set_operator_("Equal");
	tolPtr->set_value(strValue);
	tolPtr->set_effect("NoSchedule");
	//add task to the cost model
	cost_model->AddTask(task_id);
	// Get task equivalence classes.
	vector<EquivClass_t>* equiv_classes =
	cost_model->GetTaskEquivClasses(task_id);

	//set the values to the resources	
	// Create machine and its equivalence classes.
	ResourceID_t res_id = GenerateResourceID("test1");
	ResourceTopologyNodeDescriptor rtnd;
	ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
	rd_ptr->set_uuid(to_string(res_id));
	rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	::firmament::Taint* taintPtr = rd_ptr->add_taints();
	taintPtr->set_key("Test");
	taintPtr->set_value(strValue);
	taintPtr->set_effect("PreferNoSchedule");
	rd_ptr->set_max_pods(2);
	
	ResourceStatus resource_status =
	ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
	ResourceStatus* rs_ptr = &resource_status;
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));	
	cost_model->AddMachine(&rtnd);		
	cost_model->CalculateIntolerableTaintsCost(*rd_ptr,td_ptr,(*equiv_classes)[0]);
	//check are we able to add key to the tolerationSoftEqualMap
	auto it = cost_model->tolerationSoftEqualMap.find("Test");
	if(it != cost_model->tolerationSoftEqualMap.end())
		{
		EXPECT_EQ(strValue,it->second);
		}
	//one value added to the ec_to_node_priority_scores
	//we should check it is taint& tolerations
	EXPECT_EQ(1,cost_model->ec_to_node_priority_scores.size());


	EquivClass_t ec = (*equiv_classes)[0];
	auto it1 = cost_model->ec_to_node_priority_scores.find(ec);
	if(it1 != cost_model->ec_to_node_priority_scores.end())	{
		unordered_map<ResourceID_t, PriorityScoresList_t,
		          boost::hash<boost::uuids::uuid>>
		ec_to_node_priority_scoresPtr = it1->second;
		PriorityScoresList_t prio = (ec_to_node_priority_scoresPtr.find(res_id))->second;
		//score is 1
		EXPECT_EQ(1, prio.intolerable_taints_priority.score);
		//max score is -1 only this calculation happens later
		EXPECT_EQ(-1,prio.intolerable_taints_priority.final_score);
		//true value to be checked
		EXPECT_EQ(1,prio.intolerable_taints_priority.satisfy);
		}	
	}

TEST_F(CpuCostModelTest, MatchExpressionWithPodLabels){
	// Create Task.
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);

	//add task to the cost model
	cost_model->AddTask(task_id);
	// Get task equivalence classes.
	vector<EquivClass_t>* equiv_classes =
	cost_model->GetTaskEquivClasses(task_id);

	//set the values to the resources	
	// Create machine and its equivalence classes.
	ResourceID_t res_id = GenerateResourceID("test1");
	ResourceTopologyNodeDescriptor rtnd;
	ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
	rd_ptr->set_uuid(to_string(res_id));	
		rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
		
	ResourceStatus resource_status =
	ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
	ResourceStatus* rs_ptr = &resource_status;
	rd_ptr->set_max_pods(2);	
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));	
	cost_model->AddMachine(&rtnd);		
	//building the labels_map_ for checking the labels
	::firmament::Label* label = td_ptr->add_labels();
	label->set_key("Test");
	label->set_value("Testing");
	vector<TaskID_t> tasks;
	tasks.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values;
	CHECK(InsertIfNotPresent(&values, label->value(), tasks));
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label->key(), values));
	//label req
	LabelSelectorRequirement labelReq;
	labelReq.set_key("Test");
	labelReq.add_values("Testing");
	labelReq.set_operator_("In");	

	//set state to the task
	td_ptr->set_state(TaskDescriptor::RUNNING);	
	td_ptr->set_task_namespace("Test_Name_Space");
	//set the resource id 
	td_ptr->set_scheduled_to_resource(to_string(res_id));	
	//set name space value 
	cost_model->namespaces.insert("Test_Name_Space");
	//update task map task map
	InsertIfNotPresent(cost_model->task_map_.get(), task_id, td_ptr);
	
	EXPECT_EQ(true,cost_model->MatchExpressionWithPodLabels(*rd_ptr, labelReq));

	EXPECT_EQ(true,cost_model->MatchExpressionKeyWithPodLabels(*rd_ptr, labelReq));
	}

TEST_F(CpuCostModelTest, NotMatchExpressionWithPodLabels){

	// Create Task.
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);
	//add task to the cost model
	cost_model->AddTask(task_id);
	// Get task equivalence classes.
	vector<EquivClass_t>* equiv_classes =
	cost_model->GetTaskEquivClasses(task_id);

	//*** Negative testing scenario ***//
	
	//set the values to the resources	
	// Create machine and its equivalence classes.
	ResourceID_t res_id = GenerateResourceID("test1");
	ResourceTopologyNodeDescriptor rtnd;
	ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
	rd_ptr->set_uuid(to_string(res_id));	
	rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);		
	ResourceStatus resource_status =
	ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
	ResourceStatus* rs_ptr = &resource_status;
	rd_ptr->set_max_pods(2);	
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));	
	cost_model->AddMachine(&rtnd);		
	//building the labels_map_ for checking the labels
	::firmament::Label* label = td_ptr->add_labels();
	label->set_key("Test");
	label->set_value("Testing");
	vector<TaskID_t> tasks;
	tasks.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values;
	CHECK(InsertIfNotPresent(&values, label->value(), tasks));
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label->key(), values));
	//label req
	LabelSelectorRequirement labelReq;
	labelReq.set_key("Test");
	labelReq.add_values("Testing");
	labelReq.set_operator_("In");	

	//set state to the task
	td_ptr->set_state(TaskDescriptor::RUNNING);	
	td_ptr->set_task_namespace("Test_Name_Space");
	rd_ptr->set_uuid(to_string(res_id));
	td_ptr->set_scheduled_to_resource(to_string(res_id));	
	//set name space value 
	cost_model->namespaces.insert("Test_Name_Space");
	//update task map task map
	InsertIfNotPresent(cost_model->task_map_.get(), task_id, td_ptr);	

	//any label with running task is using a resource 
	//then dont allow the resource to be used. 
	//this case it is matching label->task->res id is matching with rd
	//hence return false
	EXPECT_EQ(false,cost_model->NotMatchExpressionWithPodLabels(*rd_ptr, labelReq));

	//*** Positive testing scenario***/
	
	//set the resource id 
	ResourceID_t res_id1 = GenerateResourceID("test2");
	ResourceTopologyNodeDescriptor rtnd1;
	ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
	rd_ptr1->set_uuid(to_string(res_id1));	
	rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	rd_ptr1->set_friendly_name("test2");
	ResourceVector* resource_capacity1 = rd_ptr1->mutable_resource_capacity();
	ResourceVector* available_resources1 = rd_ptr1->mutable_available_resources();
	resource_capacity1->set_cpu_cores(1000.0);
	resource_capacity1->set_ram_cap(32000);
	available_resources1->set_cpu_cores(500.0);
	available_resources1->set_ram_cap(16000);	
	ResourceStatus resource_status1 =
	ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 1);
	ResourceStatus* rs_ptr1 = &resource_status;
	rd_ptr1->set_max_pods(2);	
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1)); 
	cost_model->AddMachine(&rtnd1);		   
	td_ptr->set_scheduled_to_resource(to_string(res_id1));	

	//any label with running task is using a resource 
	//then dont allow the resource to be used. 
	//this case it is matching label->task->res id is not matching with rd
	//hence return true
	EXPECT_EQ(true,cost_model->NotMatchExpressionWithPodLabels(*rd_ptr1, labelReq));
	//testing NotMatchExpressionKeyWithPodLabels here itself
	EXPECT_EQ(true,cost_model->NotMatchExpressionKeyWithPodLabels(*rd_ptr1, labelReq));

	}
	
TEST_F(CpuCostModelTest, SatisfiesPodAntiAffinityMatchExpression){
	// Create Task.
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);

	//add task to the cost model
	cost_model->AddTask(task_id);
	// Get task equivalence classes.
	vector<EquivClass_t>* equiv_classes =
	cost_model->GetTaskEquivClasses(task_id);

	//set the values to the resources	
	// Create machine and its equivalence classes.
	ResourceID_t res_id = GenerateResourceID("test1");
	ResourceTopologyNodeDescriptor rtnd;
	ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
	rd_ptr->set_uuid(to_string(res_id));	
	rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
		
	ResourceStatus resource_status =
	ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
	ResourceStatus* rs_ptr = &resource_status;
	rd_ptr->set_max_pods(2);	
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));	
	cost_model->AddMachine(&rtnd);	


	/*** labelReq.set_operator_("In") case */
	LabelSelectorRequirementAntiAff labelSelReqAntiAff;
	labelSelReqAntiAff.set_key("Test");
	labelSelReqAntiAff.set_operator_("In");
	labelSelReqAntiAff.add_values("Testing");

	//label req
	LabelSelectorRequirement labelReq;
	labelReq.set_key("Test");
	labelReq.add_values("Testing");
	labelReq.set_operator_("In");	

	//building the labels_map_ for checking the labels
	::firmament::Label* label = td_ptr->add_labels();
	label->set_key("Test");
	label->set_value("Testing");
	vector<TaskID_t> tasks;
	tasks.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values;
	CHECK(InsertIfNotPresent(&values, label->value(), tasks));
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label->key(), values));

	//set state to the task
	td_ptr->set_state(TaskDescriptor::RUNNING); 
	td_ptr->set_task_namespace("Test_Name_Space");
	
	//set the resource id 
	td_ptr->set_scheduled_to_resource(to_string(res_id));	
	//set name space value 
	cost_model->namespaces.insert("Test_Name_Space");
	//update task map task map
	InsertIfNotPresent(cost_model->task_map_.get(), task_id, td_ptr);

	//*** Negative scenario
	//expecting false here because res_id match with resource
	EXPECT_EQ(false,cost_model->SatisfiesPodAntiAffinityMatchExpression(*rd_ptr,labelSelReqAntiAff));

	//*** positive scenario

	
	//set the resource id 
	ResourceID_t res_id1 = GenerateResourceID("test2");
	ResourceTopologyNodeDescriptor rtnd1;
	ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
	rd_ptr1->set_uuid(to_string(res_id1));	
	rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	rd_ptr1->set_friendly_name("test2");
	ResourceVector* resource_capacity1 = rd_ptr1->mutable_resource_capacity();
	ResourceVector* available_resources1 = rd_ptr1->mutable_available_resources();
	resource_capacity1->set_cpu_cores(1000.0);
	resource_capacity1->set_ram_cap(32000);
	available_resources1->set_cpu_cores(500.0);
	available_resources1->set_ram_cap(16000);	
	ResourceStatus resource_status1 =
	ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 1);
	ResourceStatus* rs_ptr1 = &resource_status;
	rd_ptr1->set_max_pods(2);	
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1)); 
	cost_model->AddMachine(&rtnd1);	
	
	//expecting false here because res_id match with resource
	EXPECT_EQ(true,cost_model->SatisfiesPodAntiAffinityMatchExpression(*rd_ptr1,labelSelReqAntiAff));

	//*** NotIn case
	LabelSelectorRequirementAntiAff labelSelReqAntiAff1;
	labelSelReqAntiAff1.set_key("Test");
	labelSelReqAntiAff1.set_operator_("NotIn");
	labelSelReqAntiAff1.add_values("Testing");
	//label req
	LabelSelectorRequirement labelReq1;
	labelReq1.set_key("Test");
	labelReq1.add_values("Testing");
	labelReq1.set_operator_("NotIn");	
	//building the labels_map_ for checking the labels
	td_ptr->clear_labels();
	::firmament::Label* label1 = td_ptr->add_labels();	
	label1->set_key("Test");
	label1->set_value("Testing");
	vector<TaskID_t> tasks1;
	tasks1.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values1;
	CHECK(InsertIfNotPresent(&values1, label1->value(), tasks1));
	cost_model->labels_map_->clear();
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label1->key(), values1));
	//true case negetive scenario
	EXPECT_EQ(true,cost_model->SatisfiesPodAntiAffinityMatchExpression(*rd_ptr,labelSelReqAntiAff1));

	//false case positive scenario
	EXPECT_EQ(false,cost_model->SatisfiesPodAntiAffinityMatchExpression(*rd_ptr1,labelSelReqAntiAff1));

	//*** Exists case	
	LabelSelectorRequirementAntiAff labelSelReqAntiAff2;
	labelSelReqAntiAff2.set_key("Test");
	labelSelReqAntiAff2.set_operator_("Exists");
	labelSelReqAntiAff2.add_values("Testing");
	//label req
	LabelSelectorRequirement labelReq2;
	labelReq2.set_key("Test");
	labelReq2.add_values("Testing");
	labelReq2.set_operator_("Exists");	
	//building the labels_map_ for checking the labels
	td_ptr->clear_labels();
	::firmament::Label* label2 = td_ptr->add_labels();	
	label2->set_key("Test");
	label2->set_value("Testing");
	vector<TaskID_t> tasks2;
	tasks2.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values2;
	CHECK(InsertIfNotPresent(&values2, label2->value(), tasks2));
	cost_model->labels_map_->clear();
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label2->key(), values2));
	//false case 
	EXPECT_EQ(false,cost_model->SatisfiesPodAntiAffinityMatchExpression(*rd_ptr,labelSelReqAntiAff2));
	//true case 
	EXPECT_EQ(true,cost_model->SatisfiesPodAntiAffinityMatchExpression(*rd_ptr1,labelSelReqAntiAff2));

	//*** DoesNotExist case	
	LabelSelectorRequirementAntiAff labelSelReqAntiAff3;
	labelSelReqAntiAff3.set_key("Test");
	labelSelReqAntiAff3.set_operator_("DoesNotExist");
	labelSelReqAntiAff3.add_values("Testing");
	//label req
	LabelSelectorRequirement labelReq3;
	labelReq3.set_key("Test");
	labelReq3.add_values("Testing");
	labelReq3.set_operator_("DoesNotExist");	
	//building the labels_map_ for checking the labels
	td_ptr->clear_labels();
	::firmament::Label* label3 = td_ptr->add_labels();	
	label3->set_key("Test");
	label3->set_value("Testing");
	vector<TaskID_t> tasks3;
	tasks3.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values3;
	CHECK(InsertIfNotPresent(&values3, label3->value(), tasks3));
	cost_model->labels_map_->clear();
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label3->key(), values3));
	//false case 
	EXPECT_EQ(true,cost_model->SatisfiesPodAntiAffinityMatchExpression(*rd_ptr,labelSelReqAntiAff3));
	//true case 
	EXPECT_EQ(false,cost_model->SatisfiesPodAntiAffinityMatchExpression(*rd_ptr1,labelSelReqAntiAff3));
	}

	
TEST_F(CpuCostModelTest, SatisfiesPodAffinityMatchExpression){
	// Create Task.
	JobDescriptor test_job;
	TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
	InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
	TaskID_t task_id = td_ptr->uid();
	td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
	td_ptr->mutable_resource_request()->set_ram_cap(1000);

	//add task to the cost model
	cost_model->AddTask(task_id);
	// Get task equivalence classes.
	vector<EquivClass_t>* equiv_classes =
	cost_model->GetTaskEquivClasses(task_id);

	//set the values to the resources	
	// Create machine and its equivalence classes.
	ResourceID_t res_id = GenerateResourceID("test1");
	ResourceTopologyNodeDescriptor rtnd;
	ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
	rd_ptr->set_uuid(to_string(res_id));	
	rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
		
	ResourceStatus resource_status =
	ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
	ResourceStatus* rs_ptr = &resource_status;
	rd_ptr->set_max_pods(2);	
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr)); 
	cost_model->AddMachine(&rtnd);	


	/*** labelReq.set_operator_("In") case */
	//label req
	LabelSelectorRequirement labelReq;
	labelReq.set_key("Test");
	labelReq.add_values("Testing");
	labelReq.set_operator_("In");	

	//building the labels_map_ for checking the labels
	::firmament::Label* label = td_ptr->add_labels();
	label->set_key("Test");
	label->set_value("Testing");
	vector<TaskID_t> tasks;
	tasks.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values;
	CHECK(InsertIfNotPresent(&values, label->value(), tasks));
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label->key(), values));

	//set state to the task
	td_ptr->set_state(TaskDescriptor::RUNNING); 
	td_ptr->set_task_namespace("Test_Name_Space");
	
	//set the resource id 
	td_ptr->set_scheduled_to_resource(to_string(res_id));	
	//set name space value 
	cost_model->namespaces.insert("Test_Name_Space");
	//update task map task map
	InsertIfNotPresent(cost_model->task_map_.get(), task_id, td_ptr);

	//expecting true here because res_id match with resource
	EXPECT_EQ(true,cost_model->SatisfiesPodAffinityMatchExpression(*rd_ptr,labelReq));

	//set the resource id 
	ResourceID_t res_id1 = GenerateResourceID("test2");
	ResourceTopologyNodeDescriptor rtnd1;
	ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
	rd_ptr1->set_uuid(to_string(res_id1));	
	rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
	rd_ptr1->set_friendly_name("test2");
	ResourceVector* resource_capacity1 = rd_ptr1->mutable_resource_capacity();
	ResourceVector* available_resources1 = rd_ptr1->mutable_available_resources();
	resource_capacity1->set_cpu_cores(1000.0);
	resource_capacity1->set_ram_cap(32000);
	available_resources1->set_cpu_cores(500.0);
	available_resources1->set_ram_cap(16000);	
	ResourceStatus resource_status1 =
	ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 1);
	ResourceStatus* rs_ptr1 = &resource_status;
	rd_ptr1->set_max_pods(2);	
	CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1)); 
	cost_model->AddMachine(&rtnd1); 
	
	//expecting false here because res_id match with resource
	EXPECT_EQ(false,cost_model->SatisfiesPodAffinityMatchExpression(*rd_ptr1,labelReq));

	// *** NotIn case
	//label req
	LabelSelectorRequirement labelReq1;
	labelReq1.set_key("Test");
	labelReq1.add_values("Testing");
	labelReq1.set_operator_("NotIn");	
	//building the labels_map_ for checking the labels
	td_ptr->clear_labels();
	::firmament::Label* label1 = td_ptr->add_labels();	
	label1->set_key("Test");
	label1->set_value("Testing");
	vector<TaskID_t> tasks1;
	tasks1.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values1;
	CHECK(InsertIfNotPresent(&values1, label1->value(), tasks1));
	cost_model->labels_map_->clear();
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label1->key(), values1));
	//false case negetive scenario
	EXPECT_EQ(false,cost_model->SatisfiesPodAffinityMatchExpression(*rd_ptr, labelReq1));

	//true case positive scenario
	EXPECT_EQ(true,cost_model->SatisfiesPodAffinityMatchExpression(*rd_ptr1, labelReq1));

	// *** Exists case	
	//label req
	LabelSelectorRequirement labelReq2;
	labelReq2.set_key("Test");
	labelReq2.add_values("Testing");
	labelReq2.set_operator_("Exists");	
	//building the labels_map_ for checking the labels
	td_ptr->clear_labels();
	::firmament::Label* label2 = td_ptr->add_labels();	
	label2->set_key("Test");
	label2->set_value("Testing");
	vector<TaskID_t> tasks2;
	tasks2.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values2;
	CHECK(InsertIfNotPresent(&values2, label2->value(), tasks2));
	cost_model->labels_map_->clear();
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label2->key(), values2));
	//false case 
	EXPECT_EQ(true,cost_model->SatisfiesPodAffinityMatchExpression(*rd_ptr,labelReq2));
	//true case 
	EXPECT_EQ(false,cost_model->SatisfiesPodAffinityMatchExpression(*rd_ptr1,labelReq2));

	// *** DoesNotExist case 
	//label req
	LabelSelectorRequirement labelReq3;
	labelReq3.set_key("Test");
	labelReq3.add_values("Testing");
	labelReq3.set_operator_("DoesNotExist");	
	//building the labels_map_ for checking the labels
	td_ptr->clear_labels();
	::firmament::Label* label3 = td_ptr->add_labels();	
	label3->set_key("Test");
	label3->set_value("Testing");
	vector<TaskID_t> tasks3;
	tasks3.push_back(task_id);
	unordered_map<string, vector<TaskID_t>> values3;
	CHECK(InsertIfNotPresent(&values3, label3->value(), tasks3));
	cost_model->labels_map_->clear();
	CHECK(InsertIfNotPresent(cost_model->labels_map_, label3->key(), values3));
	//false case 
	EXPECT_EQ(false,cost_model->SatisfiesPodAffinityMatchExpression(*rd_ptr,labelReq3));
	//true case 
	EXPECT_EQ(true,cost_model->SatisfiesPodAffinityMatchExpression(*rd_ptr1,labelReq3));	
	}
	
	

}  // namespace firmament

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
