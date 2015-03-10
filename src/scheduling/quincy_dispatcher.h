// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
#ifndef FIRMAMENT_ENGINE_QUINCY_DISPATCHER_H
#define FIRMAMENT_ENGINE_QUINCY_DISPATCHER_H

#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "scheduling/dimacs_exporter.h"
#include "scheduling/flow_graph.h"
#include "scheduling/scheduling_delta.pb.h"

namespace firmament {
namespace scheduler {

class QuincyDispatcher {
 public:
  QuincyDispatcher(shared_ptr<FlowGraph> flow_graph, bool solver_ran_once)
    : flow_graph_(flow_graph),
      solver_ran_once_(solver_ran_once),
      debug_seq_num_(0) {
  }

  multimap<uint64_t, uint64_t>* Run();
  void NodeBindingToSchedulingDelta(const FlowGraphNode& src,
                                    const FlowGraphNode& dst,
                                    map<TaskID_t, ResourceID_t>* task_bindings,
                                    SchedulingDelta* delta);

 private:
  uint64_t AssignNode(vector< map< uint64_t, uint64_t > >* extracted_flow,
                      uint64_t node);
  multimap<uint64_t, uint64_t>* GetMappings(
      vector< map< uint64_t, uint64_t > >* extracted_flow,
      unordered_set<uint64_t> leaves, uint64_t sink);
  vector< map< uint64_t, uint64_t> >* ReadFlowGraph(FILE* fptr,
                                                    uint64_t num_vertices);
  multimap<uint64_t, uint64_t>* ReadTaskMappingChanges(FILE* fptr);
  void SolverBinaryName(const string& solver, string* binary);

  shared_ptr<FlowGraph> flow_graph_;
  // DIMACS exporter for interfacing to the solver
  DIMACSExporter exporter_;
  // Boolean that indicates if the solver has knowledge of the flow graph (i.e.
  // it is set after the initial from scratch run of the solver).
  bool solver_ran_once_;
  // Debug sequence number (for solver input/output files written to /tmp)
  uint64_t debug_seq_num_;

  // FDs used to communicate with the solver.
  int outfd_[2];
  int infd_[2];
  FILE* to_solver_;
  FILE* from_solver_;
};

} // namespace scheduler
} // namespace firmament

#endif // FIRMAMENT_ENGINE_QUINCY_DISPATCHER_H
