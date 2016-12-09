package com.datatorrent.bufferserver.packet;

import com.datatorrent.netlet.util.Slice;

/**
 * Created by bhupesh on 9/12/16.
 */
public class ControlTuple extends Tuple
{
  public ControlTuple(byte[] array, int offset, int length)
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
