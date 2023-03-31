package de.inovex.airquality.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.inovex.airquality.connector.source.config.BoundingBox;
import de.inovex.airquality.data.model.SensorData;
import org.apache.commons.text.StringSubstitutor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataService {

    private final URI uri;

    private final HttpClient client;

    private LocalDateTime lastCall;

    private final long secondsBetweenCalls;

    private DataService(final URI uri, final HttpClient client, final long secondsBetweenCalls) {
        this.uri = uri;
        this.client = client;
        this.secondsBetweenCalls = secondsBetweenCalls;
    }

    private long getTimeToWait() {
        if (lastCall == null) {
            return 0;
        } else {
            LocalDateTime now = LocalDateTime.now();
            long diff = lastCall.until(now, ChronoUnit.SECONDS);
            long timeToWait = secondsBetweenCalls - diff;
            return timeToWait < 0 ? 0 : timeToWait * 1000;
        }
    }

    public List<SensorData> getData() throws IOException, InterruptedException {
        Thread.sleep(getTimeToWait());
        lastCall = LocalDateTime.now();

        final HttpRequest request = HttpRequest.newBuilder(uri)
                .GET()
                .build();

        return client.send(request, responseInfo -> {
            if(responseInfo.statusCode() != 200) {
                throw new RuntimeException(MessageFormat.format("Got HTTP {0}", responseInfo.statusCode()));
            }
            final HttpResponse.BodySubscriber<String> upstream = HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8);
            return HttpResponse.BodySubscribers.mapping(
                    upstream,
                    (String body) -> {
                        final ObjectMapper mapper = new ObjectMapper();
                        try {
                            return mapper.readValue(body, new TypeReference<List<SensorData>>() {});
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }).body();
    }


    public static DataServiceBuilder builder() {
        return new DataServiceBuilder();
    }

    public static class DataServiceBuilder {
        // KA 49.0466, 8.4935, 48.9720 8.3098
        //https://data.sensor.community/airrohr/v1/filter/box=49.0466,8.4935,48.9720,8.3098&type=SDS011

        private String baseURL = "https://data.sensor.community/airrohr/v1/filter/";

        private final String query = "box=${lat1},${lon1},${lat2},${lon2}&type=SDS011";

        private final Map<String, String> values;

        private final HttpClient client = HttpClient.newHttpClient();

        private long secondsBetweenCalls = 4 * 60L;

        private DataServiceBuilder() {
            this.values = new HashMap<>();
        }

        public DataServiceBuilder boundingBox(BoundingBox bb) {
            return this
                    .lat1(bb.getLat1())
                    .lon1(bb.getLon1())
                    .lat2(bb.getLat2())
                    .lon2(bb.getLon2());
        }

        public DataServiceBuilder lat1(float lat1) {
            values.put("lat1", Float.toString(lat1));
            return this;
        }

        public DataServiceBuilder lon1(float lon1) {
            values.put("lon1", Float.toString(lon1));
            return this;
        }

        public DataServiceBuilder lat2(float lat2) {
            values.put("lat2", Float.toString(lat2));
            return this;
        }

        public DataServiceBuilder lon2(float lon2) {
            values.put("lon2", Float.toString(lon2));
            return this;
        }

        public DataServiceBuilder baseURL(final String baseURL) {
            this.baseURL = baseURL;
            return this;
        }

        public DataServiceBuilder secondsBetweenCalls(final long seconds) {
            this.secondsBetweenCalls = seconds;
            return this;
        }

        public DataService build() throws URISyntaxException {
            final StringSubstitutor substitutor = new StringSubstitutor(values);
            URI uri = new URI(substitutor.replace(baseURL + query));

            return new DataService(uri, client, secondsBetweenCalls);
        }
    }
}
