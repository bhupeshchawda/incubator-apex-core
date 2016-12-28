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

import com.datatorrent.api.CustomControlTupleEnabledSink;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Sink;

/**
 * Default abstract implementation for OutputPort which is capable of emitting custom control tuples
 * the {@link #emitControl(UserDefinedControlTuple)} method can be used to emit control tuples onto this output port
 * Additionally this also allows setting whether or not to enable this port to propagate control tuples
 */
public class ControlAwareDefaultOutputPort<T> extends DefaultOutputPort<T>
{
  public ControlAwareDefaultOutputPort()
  {
    sink = CustomControlTupleEnabledSink.BLACKHOLE;
  }

  public void emitControl(UserDefinedControlTuple tuple)
  {
    verifyOperatorThread();
    ((CustomControlTupleEnabledSink)sink).putControl(tuple);
  }

  public boolean isConnected()
  {
    return sink != CustomControlTupleEnabledSink.BLACKHOLE;
  }

  @Override
  public void setSink(Sink<Object> s)
  {
    this.sink = (s == null ? CustomControlTupleEnabledSink.BLACKHOLE : s);
  }

}
