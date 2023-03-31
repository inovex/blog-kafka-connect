package de.inovex.airquality.data;

import com.github.tomakehurst.wiremock.WireMockServer;
import de.inovex.airquality.data.model.SensorData;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataServiceTest {

    private final WireMockServer wms = new WireMockServer();

    private final String response = "[{"
            + "    \"sensordatavalues\": ["
            + "        {"
            + "            \"id\": 14630432104,"
            + "            \"value\": \"2.60\","
            + "           \"value_type\": \"P1\""
            + "        },"
            + "        {"
            + "            \"id\": 14630432105,"
            + "            \"value\": \"2.10\","
            + "           \"value_type\": \"P2\""
            + "        }"
            + "    ],"
            + "    \"id\": 6675552884,"
            + "   \"timestamp\": \"2021-08-27 14:30:59\""
            + "}]";

    @BeforeEach
    public void setUp() {
        wms.start();
    }


    @AfterEach
    public void tearDown() {
        wms.stop();
    }

    @Test
    public void testGetData() throws Exception {
        configureFor(8080);
        stubFor(
                get("/box=1.01,2.02,3.03,4.04&type=SDS011")
                        .willReturn(ok()
                                .withBody(response))
        );
        final DataService dataService = DataService.builder()
                .lat1(1.01f)
                .lon1(2.02f)
                .lat2(3.03f)
                .lon2(4.04f)
                .baseURL("http://localhost:8080/")
                .build();
        final List<SensorData> data = dataService.getData();
        assertEquals(1, data.size());
        assertEquals(6675552884L, data.get(0).getId());
    }


    @Test
    public void testErrorFromAPI() throws Exception {
        configureFor(8080);
        stubFor(
                get("/box=1.01,2.02,3.03,4.04&type=SDS011")
                        .willReturn(notFound())
        );
        final DataService dataService = DataService.builder()
                .lat1(1.01f)
                .lon1(2.02f)
                .lat2(3.03f)
                .lon2(4.04f)
                .baseURL("http://localhost:8080/")
                .build();
        IOException exception = assertThrows(IOException.class, dataService::getData);
        assertEquals("Got HTTP 404", exception.getMessage());
    }

    @Test
    public void testWaitBetweenCalls() throws Exception {
        configureFor(8080);
        stubFor(
                get("/box=1.01,2.02,3.03,4.04&type=SDS011")
                        .willReturn(ok()
                                .withBody(response))
        );
        final DataService dataService = DataService.builder()
                .lat1(1.01f)
                .lon1(2.02f)
                .lat2(3.03f)
                .lon2(4.04f)
                .baseURL("http://localhost:8080/")
                .secondsBetweenCalls(15L)
                .build();
        final StopWatch stopWatch = StopWatch.create();
        final List<SensorData> first = dataService.getData();

        stopWatch.start();
        final List<SensorData> second = dataService.getData();
        stopWatch.stop();

        assertEquals(15L, stopWatch.getTime(TimeUnit.SECONDS));

    }


}