import com.espertech.esper.common.client.EventSender;
import com.opencsv.CSVReader;
import org.w3c.dom.Node;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class Testing_Env {

        private EPRuntime runtime;
        private EPCompiler compiler;
        private CompilerArguments argsComp;






        public Testing_Env() throws IOException {

            argsComp = new CompilerArguments();

            createConfig();

            runtime = EPRuntimeProvider.getRuntime("org.esper", argsComp.getConfiguration());
            runtime.getEventService().clockExternal();

            runtime.getEventService().advanceTime(0);
            compiler = EPCompilerProvider.getCompiler();

        }

        public void createConfig() throws IOException {

            Configuration config = new Configuration();
            config.getCommon().addEventType(InputEvent.class);
            config.getCommon().addEventType(GameIntStart.class);
            config.getCommon().addEventType(GameIntStop.class);

            argsComp.setConfiguration(config);
        }

        public void statementsSetUp(String filePath) throws EPCompileException, EPDeployException, ParserConfigurationException {

            String epl = null;
            try {
                epl = readFileLinePerLine(filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }

            EPCompiled compiled = compiler.compile(epl, argsComp);

            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);

            runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "Prova").addListener(new GameListener());

        }

        public void generateEvents(String name) throws IOException, ParseException {

            CSVReader reader = new CSVReader(new FileReader(new File(name)), ',');
            CSVReader reader1 = new CSVReader(new FileReader(new File("/Users/samuelelanghi/Documents/Polimi/anno_5/StreamProcessing/project/src/main/resources/1st_Int.csv")), ';');
            CSVReader reader2 = new CSVReader(new FileReader(new File("/Users/samuelelanghi/Documents/Polimi/anno_5/StreamProcessing/project/src/main/resources/2nd_Int.csv")), ';');


            EventSender sender = runtime.getEventService().getEventSender("InputEvent");
            EventSender sender1 = runtime.getEventService().getEventSender("GameIntStart");
            EventSender sender2 = runtime.getEventService().getEventSender("GameIntStop");

            HashMap<Long, GameIntStart> interrstarts = new HashMap<Long, GameIntStart>();
            interrstarts.put(10753295L, new GameIntStart(10753295L));

            HashMap<Long, GameIntStop> interrstops = new HashMap<Long, GameIntStop>();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            long time;
            String[] line;

            while((line=reader1.readNext()) != null){
                if(Integer.parseInt(line[0])==2010){
                    Date date = sdf.parse("1970-01-01 " +line[2]);
                    time = date.getTime()+10753295;
                    interrstarts.put(time, new GameIntStart(time));

                }
                if(Integer.parseInt(line[0])==2011){
                    Date date = sdf.parse("1970-01-01 " +line[2]);
                    time = date.getTime()+10753295;
                    interrstops.put(time, new GameIntStop(time));

                }

            }

            while((line=reader2.readNext()) != null){
                if(Integer.parseInt(line[0])==6014){
                    Date date = sdf.parse("1970-01-01 " +line[2]);
                    time = date.getTime()+13086639;
                    interrstarts.put(time, new GameIntStart(time));

                }
                if(Integer.parseInt(line[0])==6015){
                    Date date = sdf.parse("1970-01-01 " +line[2]);
                    time = date.getTime()+13086639;
                    interrstops.put(time, new GameIntStop(time));

                }

            }

            long ts;
            long tsAdv;
            long counter=0;
            HashMap<Integer, Double> throughputMultiple;
            long initialTime = System.nanoTime();
            long avgLat=0;
            long init;
            boolean finished = false;
            //Set<Long> starts = interrstarts.keySet();
            //Set<Long> stops = interrstops.keySet();
            ArrayList<Long> starts = new ArrayList<Long>(interrstarts.keySet());
            ArrayList<Long> stops = new ArrayList<Long>(interrstops.keySet());

            Collections.sort(starts);
            Collections.sort(stops);
            int counterstart=0;
            int counterstop=0;
            long minint = starts.get(0);
            long minrepr = stops.get(0);
            //Long minrepr = Collections.min(interrstops.keySet());
            boolean interrupted=false;
            boolean over1 = false;
            boolean over2 = false;
            boolean started2 = false;

            while((line=reader.readNext()) != null){



                ts = Long.parseLong(line[1]);
                tsAdv = ts / 1000000000L;

                runtime.getEventService().advanceTime(tsAdv);

                //10753295

                //10753295594424116,
                //12366783197174860

                if(!interrupted && tsAdv>=minint&&!over1){

                    sender1.sendEvent(interrstarts.get(minint));

                    counterstart++;
                    if(counterstart<starts.size()){
                        minint=starts.get(counterstart);
                    }else{over1=true;}
                    System.out.println("sent interruptions at time "+tsAdv);
                    interrupted=true;
                }

                if(interrupted && tsAdv>=minrepr&&!over2){
                    sender2.sendEvent(interrstops.get(minrepr));
                    counterstop++;

                    if(counterstop<stops.size()){
                        minrepr=stops.get(counterstop);
                    }else{over2=true;}
                    System.out.println("sent reprise at time "+tsAdv);
                    interrupted=false;
                }



                counter++;

                init = System.nanoTime();

                if(ts<12557295594424116L){
                    sender.sendEvent(new InputEvent(Integer.parseInt(line[0]),
                            ts, Long.parseLong(line[2]),
                            Long.parseLong(line[3]), Long.parseLong(line[4]),
                            Long.parseLong(line[5]), Long.parseLong(line[6]),
                            Long.parseLong(line[7]), Long.parseLong(line[8]),
                            Long.parseLong(line[9]), Long.parseLong(line[10]),
                            Long.parseLong(line[11]), Long.parseLong(line[12])));


                }else{
                    if(!finished){
                        System.out.println("FINISHED FIRST HALF.\n");
                        finished = true;
                        sender1.sendEvent(new GameIntStart(0L));
                    }

                }






                if(ts>13086639146403495L && ts<14879639146403495L) {
                    if(!started2){
                        sender2.sendEvent(new GameIntStop(0L));
                    }
                    sender.sendEvent(new InputEvent(Integer.parseInt(line[0]),
                            ts, Long.parseLong(line[2]),
                            Long.parseLong(line[3]), Long.parseLong(line[4]),
                            Long.parseLong(line[5]), Long.parseLong(line[6]),
                            Long.parseLong(line[7]), Long.parseLong(line[8]),
                            Long.parseLong(line[9]), Long.parseLong(line[10]),
                            Long.parseLong(line[11]), Long.parseLong(line[12])));

                }

                avgLat=avgLat+System.nanoTime()-init;



            }

            double dthr = (new Double(System.nanoTime()-initialTime))*10e-9;
            double count = new Double(counter);
            dthr = count/dthr;
            double dLat = ((double)avgLat)/count;

            System.out.println("Average Throughput: " +dthr+ " events/second\n" +
                    "Average Latency: "+dLat+" nanoseconds/event");


        }

        public String readFileLinePerLine(String name) throws IOException {

            BufferedReader in = new BufferedReader(new FileReader(new File(name)));

            StringBuilder builder = new StringBuilder();
            String line;

            while((line=in.readLine()) != null){

                builder.append(line);
                builder.append("\n");

            }

            builder.setLength(builder.length()-1);

            line = builder.toString();


            return line;
        }

}
