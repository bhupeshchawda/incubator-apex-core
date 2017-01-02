package com.datatorrent.stram.engine;

import com.datatorrent.api.ControlSink;
import com.datatorrent.stram.tuple.CustomControlTuple;

/**
 * Created by bhupesh on 2/1/17.
 */
public abstract class DefaultControlSink<T> implements ControlSink<T>
{
  private boolean propogateControlTuples = true; // default

  @Override
  public void putControl(Object payload)
  {
    put((T)new CustomControlTuple(payload));
  }

  @Override
  public boolean propogateControlTuples()
  {
    return propogateControlTuples;
  }

  public void setPropogateControlTuples(boolean propogate)
  {
    this.propogateControlTuples = propogate;
  }

}
