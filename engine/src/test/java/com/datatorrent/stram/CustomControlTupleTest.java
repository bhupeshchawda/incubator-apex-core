package com.datatorrent.stram;

import java.io.IOException;
import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class CustomControlTupleTest
{
  public static final Logger LOG = LoggerFactory.getLogger(CustomControlTupleTest.class);
  private static final int TEST_FOR_NUM_WINDOWS = 5;
  private static long controlIndex = 0;
  private static long dataIndex = 0;
  private static int numDataTuples = 0;
  private static int numControlTuples = 0;
  private static int dataAfterControl = 0;
  private static long numWindows = 0;
  private static boolean run = true;
  private static boolean endApp = false;

  public static class Generator extends BaseOperator implements InputOperator
  {
    private boolean sendControl = false;
    @OutputPortFieldAnnotation(propogateControlTuples = false)
    public final transient ControlAwareDefaultOutputPort<Double> out = new ControlAwareDefaultOutputPort<>();

    @Override
    public void beginWindow(long windowId)
    {
      if (run) {
        out.emitControl(new Long(controlIndex++));
        sendControl = true;
      }
    }

    @Override
    public void emitTuples()
    {
      if (run) {
        out.emit(new Double(dataIndex++));
        if (sendControl) {
          out.emitControl(new Long(controlIndex++));
          sendControl = false;
        }
      }
    }

    @Override
    public void endWindow()
    {
      if (run) {
        out.emitControl(new Long(controlIndex++));
        if (numWindows++ > TEST_FOR_NUM_WINDOWS) {
          run = false;
        }
      }
    }
  }

  public static class Processor extends BaseOperator
  {
    private boolean receivedControlThisWindow = false;
    private long currentWindowId;

    public final transient ControlAwareDefaultInputPort<Double> input = new ControlAwareDefaultInputPort<Double>()
    {
      @Override
      public void process(Double tuple)
      {
        if (receivedControlThisWindow) {
          dataAfterControl++;
        }
        numDataTuples++;
      }

      @Override
      public void processControl(Object tuple)
      {
        numControlTuples++;
        receivedControlThisWindow = true;
      }
    };

    @Override
    public void beginWindow(long windowId)
    {
      currentWindowId = windowId;
      receivedControlThisWindow = false;
    }

    @Override
    public void endWindow()
    {
      receivedControlThisWindow = false;
      if (!run) {
        endApp = true;
      }
    }
  }

  @ApplicationAnnotation(name="CustomControlTupleTest")
  public static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      Generator randomGenerator = dag.addOperator("randomGenerator", Generator.class);
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
          return endApp;
        }
      });

      lc.run(10000); // runs for 10 seconds and quits

      LOG.info("Data Tuples {} Data Index {}", numDataTuples, dataIndex);
      LOG.info("Control Tuples {} Control Index {}", numControlTuples, controlIndex);
      Assert.assertTrue("Incorrect Data Tuples", numDataTuples == dataIndex); ;
      Assert.assertTrue("Incorrect Control Tuples", numControlTuples == controlIndex);
      Assert.assertTrue("Data tuples received after control tuples in window", dataAfterControl == 0);

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
