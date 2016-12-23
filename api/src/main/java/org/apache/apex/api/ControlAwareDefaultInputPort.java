package org.apache.apex.api;

import com.datatorrent.api.DefaultInputPort;

public abstract class ControlAwareDefaultInputPort<T> extends DefaultInputPort<T>
{
  @Override
  public void putControl(Object tuple)
  {
    count++;
    processControl(tuple);
  }

  public abstract void processControl(Object tuple);
}
