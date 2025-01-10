/**
 * Copyright (c) 2012, Akamai Technologies
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Akamai Technologies nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "CompileTimeLogicalOperator.hh"
#include "ConstantScan.hh"
#include "RuntimeProcess.hh"

CompileTimeLogicalOperator::CompileTimeLogicalOperator(bool constantScan)
  :
  mConstantScan(constantScan)
{
}

void CompileTimeLogicalOperator::create(class RuntimePlanBuilder& plan)
{
  if (mConstantScan) {
    // Execute the operator right here and now
    // and then put the result into a constant scan
    // TODO: Think more deeply about this behavior,
    // genericize and make part of the graph language.
    // It seems that it should be sensible to take an
    // arbitrary subgraph with no input edges and mark
    // it for compile time execution.  Maybe this is just
    // how sequential execution works in a map reduce context.
    RuntimePlanBuilder tmpBld;
    internalCreate(tmpBld);
    RuntimeOperatorPlan tmpPlan(1, true);
    BOOST_ASSERT(std::distance(tmpBld.begin_operator_types(),
			       tmpBld.end_operator_types()) == 1);
    tmpPlan.addOperatorType(*tmpBld.begin_operator_types());
    RuntimeConstantSinkOperatorType * sink = 
      new RuntimeConstantSinkOperatorType(getOutput(0)->getRecordType());
    tmpPlan.addOperatorType(sink);
    tmpPlan.connectStraight(*tmpBld.begin_operator_types(),
			    0, sink, 0, true, true);
    RuntimeProcess p;
    p.init(0,0,1,tmpPlan);
    p.run();
    // OK.  Now we can create the constant scan and
    // insert into the plan.
    RuntimeConstantScanOperatorType * scan = 
      new RuntimeConstantScanOperatorType(getOutput(0)->getRecordType(),
					  sink->getSink());
    plan.addOperatorType(scan);
    plan.mapOutputPort(this, 0, scan, 0);  
  } else {
    // Normal execution.
    internalCreate(plan);
  }
}

