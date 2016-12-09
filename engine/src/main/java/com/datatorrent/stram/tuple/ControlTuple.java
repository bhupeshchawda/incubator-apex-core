package com.datatorrent.stram.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 * Created by bhupesh on 9/12/16.
 */
public class ControlTuple extends Tuple
{
  private Object userObject;

  public ControlTuple(Object userObject)
  {
    super(MessageType.CUSTOM_CONTROL, 0);
    this.userObject = userObject;
  }

  public Object getUserObject()
  {
    return userObject;
  }

  @Override
  public long getWindowId()
  {
    throw new UnsupportedOperationException("Not supported yet");
  }
}
