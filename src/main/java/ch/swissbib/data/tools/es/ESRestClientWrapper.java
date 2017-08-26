package ch.swissbib.data.tools.es;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;

public class ESRestClientWrapper {

    private RestClient restClient;

    public ESRestClientWrapper(String host, Integer port, String schema) {
        this.restClient = RestClient.builder(
                new HttpHost(host, port, schema)).build();
    }

    public ESRestClientWrapper(String host, Integer port,
                               String user, String password,
                               String schema) {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, password));


        this.restClient = RestClient.builder(new HttpHost(host, port,schema))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                })
                .build();
    }

    public RestClient getRestClient() {
        return this.restClient;
    }

    public void close() throws IOException{
        if (null != this.restClient) {
            this.restClient.close();
        }
    }

}
