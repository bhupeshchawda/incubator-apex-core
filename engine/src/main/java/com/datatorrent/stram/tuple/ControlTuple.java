package com.datatorrent.stram.tuple;

import java.util.UUID;

import com.datatorrent.bufferserver.packet.MessageType;

public class ControlTuple extends Tuple
{
  private Object userObject;
  private UUID id;

  public ControlTuple(Object userObject)
  {
    super(MessageType.CUSTOM_CONTROL, 0);
    this.userObject = userObject;
    id = UUID.randomUUID();
  }

  public Object getUserObject()
  {
    return userObject;
  }

  public UUID getId()
  {
    return id;
  }
}
