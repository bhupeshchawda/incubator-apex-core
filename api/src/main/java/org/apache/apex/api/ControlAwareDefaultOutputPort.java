package org.apache.apex.api;

import com.datatorrent.api.ControlSink;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Sink;

/**
 * Default abstract implementation for {@link Operator.OutputPort} which is capable of emitting custom control tuples
 * the {@link #emitControl(Object)} method can be used to emit control tuples onto this output port
 * Additionally this also allows setting whether or not to enable this port to propogate control tuples
 */
public class ControlAwareDefaultOutputPort<T> extends DefaultOutputPort<T>
{
  private ControlSink<Object> sink;

  public ControlAwareDefaultOutputPort()
  {
    sink = ControlSink.BLACKHOLE;
  }

  public void emitControl(Object tuple)
  {
    // operatorThread could be null if setup() never got called.
    if (operatorThread != null && Thread.currentThread() != operatorThread) {
      // only under certain modes: enforce this
      throw new IllegalStateException("Current thread " + Thread.currentThread().getName() +
        " is different from the operator thread " + operatorThread.getName());
    }
    if (tuple instanceof ControlTuple) {
      sink.putControl(tuple);
    } else {
      throw new IllegalArgumentException("Expecting an instance of ControlTuple");
    }
  }

  public boolean isConnected()
  {
    return sink != Sink.BLACKHOLE;
  }

  @Override
  public void setSink(Sink<Object> s)
  {
    this.sink = (ControlSink<Object>)(s == null ? ControlSink.BLACKHOLE : s);
  }

  @Override
  public ControlSink<Object> getSink()
  {
    return sink;
  }

  /**
   * Set whether or not to propogate custom control tuples generated upstream to downstream operators
   *
   * @param propogate whether or not to propogate
   */
  public void setPropogateControlTuples(boolean propogate)
  {
    sink.setPropogateControlTuples(propogate);
  }

}
