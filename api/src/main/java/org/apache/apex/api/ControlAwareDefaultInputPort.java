package org.apache.apex.api;

import com.datatorrent.api.DefaultInputPort;

public abstract class ControlAwareDefaultInputPort<T> extends DefaultInputPort<T>
{
  @Override
  public void put(T tuple)
  {
    if (tuple instanceof ControlTupleWrapper) {
      count++;
      processControl(((ControlTupleWrapper)tuple).getPayload());
    } else {
      super.put(tuple);
    }
  }

  public abstract void processControl(Object tuple);
}
