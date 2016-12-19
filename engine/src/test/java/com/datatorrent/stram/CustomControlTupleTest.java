package com.datatorrent.stram;

import java.io.IOException;
import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.CustomControlTuple;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class CustomControlTupleTest
{
  private static int numDataTuples = 0;
  private static int numControlTuples = 0;
  private static int inWindowControlTuples = 0;

  public static class RandomNumberGenerator extends BaseOperator implements InputOperator
  {
    private boolean dataSent = false;
    private boolean singleWindow = false;
    public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

    @Override
    public void beginWindow(long windowId)
    {
      if (!singleWindow) {
        out.emitControl(new CustomControlTuple(new Integer(1)));
      }
    }

    @Override
    public void emitTuples()
    {
      if (!dataSent) {
        out.emit(Math.random());
        out.emitControl(new CustomControlTuple(new Integer(2)));
        dataSent = true;
      }
    }

    @Override
    public void endWindow()
    {
      if (!singleWindow) {
        out.emitControl(new CustomControlTuple(new Integer(3)));
        singleWindow = true;
      }
    }
  }

  public static class Processor extends BaseOperator
  {
    private boolean inWindow = false;

    public final transient DefaultInputPort<Double> input = new DefaultInputPort<Double>()
    {
      @Override
      public void process(Double tuple)
      {
        numDataTuples++;
        System.out.println("Received Data Tuple: " + tuple);
      }

      @Override
      public void processControl(CustomControlTuple tuple)
      {
        numControlTuples++;
        if (inWindow) {
          inWindowControlTuples++;
        }
        System.out.println("Received control Tuple: " + tuple);
      }
    };

    @Override
    public void beginWindow(long windowId)
    {
      inWindow = true;
    }

    @Override
    public void endWindow()
    {
      inWindow = false;
    }
  }

  @ApplicationAnnotation(name="CustomControlTupleTest")
  public static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
      Processor processor = dag.addOperator("process", new Processor());
      dag.addStream("randomData", randomGenerator.out, processor.input);
    }
  }

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          return numControlTuples >= 3;
        }
      });

      lc.run(1000000); // runs for 10 seconds and quits

      Assert.assertTrue(numDataTuples == 1 && numControlTuples == 3 && inWindowControlTuples == 3);
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
