package com.datatorrent.api;

/**
 * Created by bhupesh on 28/12/16.
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
    public boolean propogateControlTuples()
    {
      return false;
    }

    @Override
    public void setPropogateControlTuples(boolean propogate)
    {
    }
  };

  public void putControl(Object payload);

  public boolean propogateControlTuples();

  public void setPropogateControlTuples(boolean propogate);
}
