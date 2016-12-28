package com.datatorrent.stram.tuple;

import java.util.UUID;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 * Created by bhupesh on 28/12/16.
 */
public class CustomControlTuple extends Tuple
{
  private Object userObject;
  @FieldSerializer.Bind(JavaSerializer.class)
  private UUID id;

  public CustomControlTuple()
  {
    super(MessageType.CUSTOM_CONTROL, 0);
  }

  public CustomControlTuple(Object userObject)
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