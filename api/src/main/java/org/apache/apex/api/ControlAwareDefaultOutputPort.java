/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.api;

import com.datatorrent.api.ControlSink;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Sink;

/**
 * Default abstract implementation for {@link Operator.OutputPort} which is capable of emitting custom control tuples
 * the {@link #emitControl(Object)} method can be used to emit control tuples onto this output port
 * Additionally this also allows setting whether or not to enable this port to propogate control tuples
 */
public class ControlAwareDefaultOutputPort<T> extends DefaultOutputPort<T>
{
  private ControlSink<Object> sink;

  public ControlAwareDefaultOutputPort()
  {
    sink = ControlSink.BLACKHOLE;
  }

  public void emitControl(Object tuple)
  {
    // operatorThread could be null if setup() never got called.
    if (operatorThread != null && Thread.currentThread() != operatorThread) {
      // only under certain modes: enforce this
      throw new IllegalStateException("Current thread " + Thread.currentThread().getName() +
        " is different from the operator thread " + operatorThread.getName());
    }
    if (tuple instanceof ControlTuple) {
      sink.putControl(tuple);
    } else {
      throw new IllegalArgumentException("Expecting an instance of ControlTuple");
    }
  }

  public boolean isConnected()
  {
    return sink != Sink.BLACKHOLE;
  }

  @Override
  public void setSink(Sink<Object> s)
  {
    this.sink = (ControlSink<Object>)(s == null ? ControlSink.BLACKHOLE : s);
  }

  @Override
  public ControlSink<Object> getSink()
  {
    return sink;
  }

  /**
   * Set whether or not to propogate custom control tuples generated upstream to downstream operators
   *
   * @param propogate whether or not to propogate
   */
  public void setPropogateControlTuples(boolean propogate)
  {
    sink.setPropogateControlTuples(propogate);
  }

}
