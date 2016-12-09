package com.datatorrent.bufferserver.packet;

import org.apache.apex.api.MessageType;

import com.datatorrent.netlet.util.Slice;

/**
 *
 */
public class CustomControlTuple extends Tuple
{
  public CustomControlTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  @Override
  public int getWindowId()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getBaseSeconds()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getPartition()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getWindowWidth()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Slice getData()
  {
    return new Slice(buffer, offset + 1, length - 1);
  }

  public static byte[] getSerializedTuple(Slice f)
  {
    byte[] array = new byte[f.length + 1];
    array[0] = MessageType.CUSTOM_CONTROL_VALUE;
    System.arraycopy(f.buffer, f.offset, array, 1, f.length);
    return array;
  }

}
