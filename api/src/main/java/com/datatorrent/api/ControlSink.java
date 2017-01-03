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
package com.datatorrent.api;

/**
 * A {@link Sink} which supports adding control tuples
 * Additionally allows to set and retrieve propogation information for control tuples
 */
public interface ControlSink<T> extends Sink<T>
{
  public static final ControlSink<Object> BLACKHOLE = new ControlSink<Object>()
  {
    @Override
    public void put(Object tuple)
    {
    }

    @Override
    public void putControl(Object payload)
    {
    }

    @Override
    public int getCount(boolean reset)
    {
      return 0;
    }

    @Override
    public boolean isPropogateControlTuples()
    {
      return false;
    }

    @Override
    public void setPropogateControlTuples(boolean propogate)
    {
    }
  };

  /**
   * Add a control tuple to the sink
   *
   * @param payload the control tuple payload
   */
  public void putControl(Object payload);

  /**
   * Identify whether this sink allows custom control tuples to be propogated
   *
   * @return true if custom control tuples should be forwarded to this sink; false otherwise
   */
  public boolean isPropogateControlTuples();

  /**
   * Set propogation information for custom control tuples on this sink
   *
   * @param propogate whether or not to forward custom control tuples to this sink
   */
  public void setPropogateControlTuples(boolean propogate);
}
