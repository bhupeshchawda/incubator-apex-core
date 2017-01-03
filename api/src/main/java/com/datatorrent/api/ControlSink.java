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
    public boolean propogateControlTuples()
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
  public boolean propogateControlTuples();

  /**
   * Set propogation information for custom control tuples on this sink
   *
   * @param propogate whether or not to forward custom control tuples to this sink
   */
  public void setPropogateControlTuples(boolean propogate);
}
