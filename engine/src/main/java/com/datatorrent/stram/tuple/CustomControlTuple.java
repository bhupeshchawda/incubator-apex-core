package com.datatorrent.stram.tuple;

import org.apache.apex.api.MessageType;

public class CustomControlTuple extends Tuple
{
  private Object userObject;

  public CustomControlTuple(Object userObject)
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
