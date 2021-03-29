import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.runtime.client.EPDeployException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Main {

    public static void main(String[] args) throws IOException, ParseException {

        Testing_Env env = new Testing_Env();

        try {
            env.statementsSetUp("/Users/samuelelanghi/Documents/Polimi/anno_5/StreamProcessing/" +
                    "project/src/main/resources/querys.epl");
        } catch (EPCompileException e) {
            e.printStackTrace();
        } catch (EPDeployException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }



        env.generateEvents("/Users/samuelelanghi/Documents/Polimi/anno_5/StreamProcessing/full-game.csv");

    }
}
