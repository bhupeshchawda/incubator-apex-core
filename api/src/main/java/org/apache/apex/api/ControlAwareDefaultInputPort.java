package org.apache.apex.api;

import com.datatorrent.api.ControlSink;
import com.datatorrent.api.DefaultInputPort;

/**
 * Created by bhupesh on 28/12/16.
 */
public abstract class ControlAwareDefaultInputPort<T> extends DefaultInputPort<T> implements ControlSink<T>
{
  @Override
  public void putControl(Object payload)
  {
    count++;
    processControl(payload);
  }

  public abstract void processControl(Object payload);
}