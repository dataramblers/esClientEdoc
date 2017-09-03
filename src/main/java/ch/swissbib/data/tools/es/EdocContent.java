package ch.swissbib.data.tools.es;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.cli.*;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import java.io.IOException;
import java.util.ArrayList;
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
            //int i = 0;
            boolean smallOutput = userArgs.get("endpoint").equalsIgnoreCase("edoc_nested") &&  Boolean.valueOf( userArgs.get("concise"));

            while (true) {

                if (smallOutput ? printWhenAvailableSmall(wholeResponse) : printWhenAvailable(wholeResponse) ) {
                    response = restWrapper.getRestClient().
                            performRequest("GET", webApp + "/_search/scroll",
                                    Collections.<String, String>emptyMap(),
                                    new NStringEntity(mQ.getBody(from,10,scrollId),ContentType.APPLICATION_JSON));
                    wholeResponse = getNodeFromResponse(response);
                    scrollId = wholeResponse.get("_scroll_id").toString();
                } else {
                    break;
                }
                //i++;
                //System.out.println(i);
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

        args.put("concise", cmd.hasOption("concise") ? "true" : "false");


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

        o = new Option("c","concise",false,"concise (small) outputput for Hackathon");
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


    private static boolean printWhenAvailableSmall (JsonNode node) {
        JsonNode allhits = node.findPath("hits").findPath("hits");
        Iterator<JsonNode> singleHits = allhits.elements();
        boolean available = false;

        ObjectMapper mapper = new ObjectMapper();
        while (singleHits.hasNext()) {
            ObjectNode newRoot = mapper.createObjectNode();
            available = true;
            JsonNode _source = singleHits.next().get("_source");
            checkAndAddArrayStructure(newRoot,_source,"creators",mapper);
            checkAndAddArrayStructure(newRoot,_source,"editors",mapper);
            checkAndAddKeyValue(newRoot,_source,"title");
            checkAndAddKeyValue(newRoot,_source,"isbn");
            checkAndAddKeyValue(newRoot,_source,"isbn_e");
            checkAndAddKeyValue(newRoot,_source,"issn");
            checkAndAddKeyValue(newRoot,_source,"issn_e");
            checkAndAddKeyValue(newRoot,_source,"eprintid");
            checkAndPutIdNumber(newRoot,_source);
            System.out.println(newRoot.toString());
        }
        return available;
    }






    private static void checkAndAddArrayStructure(ObjectNode root, JsonNode toBeChecked,
                                     String fieldName, ObjectMapper mapper) {
        if (toBeChecked.has(fieldName)) {
            ArrayNode newArrayStructure = root.putArray(fieldName);
            Iterator<JsonNode> iElements = toBeChecked.findPath(fieldName).elements();
            while (iElements.hasNext()) {
                JsonNode cN = iElements.next();
                ObjectNode oN = mapper.createObjectNode();
                if (cN.get("name").has("family")) {
                    oN.put("family", cN.get("name").get("family").textValue());
                } else {
                    oN.put("family", "");
                }
                if (cN.get("name").has("given")) {
                    oN.put("given", cN.get("name").get("given").textValue());
                } else {
                    oN.put("given", "");
                }
                newArrayStructure.add(oN);
            }
        }

    }

    private static void checkAndAddKeyValue(ObjectNode root, JsonNode toBeChecked,
                                                  String fieldName) {
        if (toBeChecked.has(fieldName) && toBeChecked.get(fieldName).textValue() != null) {
            root.put(fieldName,toBeChecked.get(fieldName).textValue());
        }

    }

    private static void checkAndPutIdNumber(ObjectNode root, JsonNode toBeChecked) {

        ArrayList<JsonNode> nodesList = new ArrayList<>();
        if (toBeChecked.has("id_number")) {
            Iterator<JsonNode> iIds = toBeChecked.get("id_number").elements();
            while (iIds.hasNext()) {
                try {
                    JsonNode id = iIds.next();
                    if (null != id && id.has("type") && null != id.get("type") &&
                            id.has("id") && null != id.get("id").asText()) {
                        if (id.get("type").asText().equalsIgnoreCase("doi")) {
                                root.put("doi", id.get("id").asText());
                        } else if (id.get("type").asText().equalsIgnoreCase("pmid")) {
                                root.put("pmid", id.get("id").asText());
                        }
                    }
                } catch (Exception ex) {
                    System.err.println(ex.getLocalizedMessage());
                }
            }

        }

    }


}
