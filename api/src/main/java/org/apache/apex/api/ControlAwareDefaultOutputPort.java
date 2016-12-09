package org.apache.apex.api;

import com.datatorrent.api.DefaultOutputPort;

public class ControlAwareDefaultOutputPort<T> extends DefaultOutputPort<T>
{
  public void emitControl(Object tuple)
  {
    // operatorThread could be null if setup() never got called.
    if (operatorThread != null && Thread.currentThread() != operatorThread) {
      // only under certain modes: enforce this
      throw new IllegalStateException("Current thread " + Thread.currentThread().getName() +
        " is different from the operator thread " + operatorThread.getName());
    }
    sink.put(new ControlTupleWrapper(tuple));
  }
}
