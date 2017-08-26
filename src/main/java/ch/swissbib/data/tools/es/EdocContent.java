package ch.swissbib.data.tools.es;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

public class EdocContent
{
    public static void main( String[] args ) throws IOException
    {

        HashMap<String, String> userArgs = getArgs(getCommandLine(getOptions(),args));
        ESRestClientWrapper restWrapper = getRestWrapper(userArgs);


        MatchAllQuery mQ = new MatchAllQuery();


        //edoc_nested oder edoc_flattened
        try {

            String endpoint = userArgs.get("endpoint");
            String scrollTime = userArgs.get("time");
            String webApp = userArgs.get("app");


            Response response = restWrapper.getRestClient().
                    performRequest("GET", webApp + "/" + endpoint + "/_search?scroll=" + scrollTime,
                    Collections.<String, String>emptyMap(),
                    new NStringEntity(mQ.getBody(),ContentType.APPLICATION_JSON));

            JsonNode wholeResponse = getNodeFromResponse(response);

            String scrollId = wholeResponse.get("_scroll_id").toString();
            Integer from = 10;
            while (true) {
                if (printWhenAvailable(wholeResponse)) {
                    response = restWrapper.getRestClient().
                            performRequest("GET", webApp + "/_search/scroll",
                                    Collections.<String, String>emptyMap(),
                                    new NStringEntity(mQ.getBody(from,10,scrollId),ContentType.APPLICATION_JSON));
                    wholeResponse = getNodeFromResponse(response);
                    scrollId = wholeResponse.get("_scroll_id").toString();
                } else {
                    break;
                }
            }


            restWrapper.close();

        } catch (IOException ex) {
            ex.printStackTrace();
            restWrapper.close();
        }



    }

    private static HashMap<String, String> getArgs(CommandLine cmd) {
        HashMap<String,String> args = new HashMap<>();
        args.put("host", cmd.getOptionValue("host"));
        args.put("port",cmd.getOptionValue("port"));

        if (cmd.hasOption("user")) {
            args.put("user",cmd.getOptionValue("user"));
        }

        if (cmd.hasOption("password")) {
            args.put("password",cmd.getOptionValue("password"));
        }

        args.put("schema", cmd.getOptionValue("schema","http"));
        args.put("endpoint", cmd.getOptionValue("endpoint","edoc_nested"));

        args.put("time", cmd.getOptionValue("time","5m"));

        args.put("app", cmd.getOptionValue("app","/es"));



        return args;
    }

    private static Options getOptions() {
        Options options = new Options();
        Option o = new Option("h","host",true,"ElasticSearch Host");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("p","port",true,"Port on ElasticSearch Host");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("U","user",true,"user basic authentication");
        o.setRequired(false);
        options.addOption(o);
        o = new Option("P","password",true,"user password for basic authentication");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("s","schema",true,"schema for ES client");
        o.setRequired(false);

        options.addOption(o);

        o = new Option("h","help",false,"help Option");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("e","endpoint",true,"ES path (includes index and optional type) no ES endpoint");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("t","time",true,"definition of scroll time - e.g. 5m");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("a","app",true,"path of the web application");
        o.setRequired(false);
        options.addOption(o);


        return options;

    }

    private static ESRestClientWrapper getRestWrapper(HashMap<String, String> userArgs) {

        ESRestClientWrapper restWrapper = null;
        if (!userArgs.containsKey("user") && !userArgs.containsKey("password")) {

            restWrapper = new ESRestClientWrapper(
                    userArgs.get("host"),
                    Integer.valueOf(userArgs.get("port")),
                    userArgs.get("schema")
            );
        } else {

            restWrapper = new ESRestClientWrapper(
                    userArgs.get("host"),
                    Integer.valueOf(userArgs.get("port")),
                    userArgs.get("user"),
                    userArgs.get("password"),
                    userArgs.get("schema")
            );
        }

        return restWrapper;
    }

    private static CommandLine getCommandLine(Options options, String[] args) {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {

            cmd = parser.parse(options, args);
            if (cmd.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("EdocContent", options);
                System.exit(0);
            }


        } catch (ParseException pe) {
            pe.printStackTrace();
            System.exit(-1);
        }

        return cmd;

    }

    private static JsonNode getNodeFromResponse(Response response) throws IOException{

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonParser jp = factory.createParser(response.getEntity().getContent());
        return mapper.readTree(jp);

    }

    private static boolean printWhenAvailable (JsonNode node) {
        JsonNode allhits = node.findPath("hits").findPath("hits");
        Iterator<JsonNode> singleHits = allhits.elements();
        boolean available = false;
        while (singleHits.hasNext()) {
            available = true;
            JsonNode hit = singleHits.next();
            System.out.println(hit.get("_source").toString());
        }
        return available;
    }

}
