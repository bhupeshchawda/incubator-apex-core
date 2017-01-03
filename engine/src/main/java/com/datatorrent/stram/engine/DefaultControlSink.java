package com.datatorrent.stram.engine;

import com.datatorrent.api.ControlSink;
import com.datatorrent.stram.tuple.CustomControlTuple;

/**
 * A default implementation for {@link ControlSink}
 */
public abstract class DefaultControlSink<T> implements ControlSink<T>
{
  private boolean propogateControlTuples = true; // default

  /**
   * {@inheritDoc}
   */
  @Override
  public void putControl(Object payload)
  {
    put((T)new CustomControlTuple(payload));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean propogateControlTuples()
  {
    return propogateControlTuples;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setPropogateControlTuples(boolean propogate)
  {
    this.propogateControlTuples = propogate;
  }

}
