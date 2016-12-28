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

  };

  public void putControl(Object payload);
}
