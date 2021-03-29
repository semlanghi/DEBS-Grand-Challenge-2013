import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import org.slf4j.LoggerFactory;



public class GameListener implements UpdateListener {


    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(GameListener.class);

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {


        //System.out.println(runtime.getEventService().getCurrentTime());


        for(EventBean e : newEvents){

            logger.info(e.getUnderlying().toString());

        }

    }
}
