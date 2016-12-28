package org.apache.apex.api;

import com.datatorrent.api.ControlSink;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Sink;

/**
 * Created by bhupesh on 28/12/16.
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
}
