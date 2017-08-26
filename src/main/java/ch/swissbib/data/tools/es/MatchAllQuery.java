package ch.swissbib.data.tools.es;

import org.apache.http.entity.ContentType;

public class MatchAllQuery {


    private String scrollTime;

    public MatchAllQuery() {
        this.scrollTime = "5m";
    }

    public MatchAllQuery(String scrollTime) {
        this.scrollTime = scrollTime;
    }


    public String getBody(Integer from, Integer size, String scrollId) {

        String matchAllMinimalFromSize = "{\n" +
                "     \"scroll\": \"" + scrollTime + "\"," +
                //"     \"scroll_id\": " + scrollId + "," +
                "     \"scroll_id\": " + scrollId +
                //"     \"from\": %s," +
                //"     \"size\": %s," +
                //"     \"query\": {" +
                //"        \"match_all\": {}" +
                //"     }" +
                "}";

        //String test =  String.format(matchAllMinimalFromSize, from, size);
        return matchAllMinimalFromSize;
    }


    public String getBody() {
       return  "{\n" +
                "     \"query\": {" +
                "        \"match_all\": {}" +
                "     }" +
                "}";
    }


}
