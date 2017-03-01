package org.fiolino.indexer.analytics;

import static org.fiolino.indexer.analytics.AnalyticsService.AnalyticsWebService;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fiolino.indexer.domain.Mandator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.mls.common.test.categories.UnitTest;
import org.fiolino.UnitTestCase;
import com.mls.domain.analytics.model.AnalyticsRequest;
import com.mls.domain.analytics.model.AnalyticsResponse;
import com.mls.domain.solr.model.analytics.AnalyticsModel;

@RunWith(MockitoJUnitRunner.class)
public class AnalyticsServiceTest extends UnitTestCase {

    public static final String EXPECTED_CONTENT_TYPE = "Content-Type";
    public static final Long EXPECTED_ID = 9876L;

    public static final String EXPECTED_TEXT = "Model-Text";
    public static final String EXPECTED_MODEL_VERSION = "Model-Version";
    public static final String EXPECTED_LANGUAGE = "Language";

    public static final String WEB_SERVICE_URL = "http://localhost/web-service/";

    public static final Mandator MANDATOR = new Mandator(11223344L, "Mandator");

    @Spy
    private final AnalyticsService service = new AnalyticsService(EXPECTED_MODEL_VERSION, EXPECTED_LANGUAGE);

    @Mock
    private AnalyticsWebService mockedWebService;

    @Test
    public void itShouldRequestWebServices_ToFetchAnalyticsData() throws Exception {
        // given
        defineWebService(mockedWebService);
        defineWebServicesData("Concept", new String[]{"value"});

        // when
        BunchOfAnalyticsData data = service.fetchData(MANDATOR, createModelWithExpectedValues());

        // then
        assertTrue("Result is not empty", !data.getAnalysisMap().isEmpty());
        assertThat("Contains the concept", data.getAnalysisMap().keySet().iterator().next(), equalTo("Concept"));
        assertThat("Contains the value", data.getAnalysisMap().values().iterator().next(), equalTo(Arrays.asList("value")));
        assertTrue("No other Concepts or values", data.getAnalysisMap().size() == 1);
    }

    private void defineWebService(AnalyticsWebService instance) {
        doReturn(instance).when(service).createWebService(any(Mandator.class));
    }

    private void defineWebServicesData(String concept, String[] values) throws Exception {
        AnalyticsResponse response = createResponse(concept, values);
        when(mockedWebService.doAnalytics(any(AnalyticsRequest.class))).thenReturn(response);
    }

    private AnalyticsResponse createResponse(String concept, String[] values) {
        Map<String, List<String>> data = createData(concept, values);

        AnalyticsResponse response = mock(AnalyticsResponse.class);
        when(response.getAnalysisMap()).thenReturn(data);

        return response;
    }

    private Map<String, List<String>> createData(String concept, String[] values) {
        Map<String, List<String>> map = new HashMap<>();
        map.put(concept, Arrays.asList(values));

        return map;
    }

    @Test
    public void itShouldCreateANewInstanceOfWebService() throws Exception {
        // when
        AnalyticsWebService concreteWebService = service.createWebService(MANDATOR);

        // then
        assertTrue("Not null", concreteWebService != null);
        assertTrue("Not a mock", !concreteWebService.equals(mockedWebService));
    }

    @Test
    public void itShouldPrepareAnalyticsRequest() throws Exception {
        // when
        AnalyticsRequest request = service.createRequest(MANDATOR, createModelWithExpectedValues());

        // then
        assertEquals(MANDATOR.getBaseName(), request.getMandatorId());
        assertEquals(EXPECTED_ID.toString(), request.getDocumentId());
        assertEquals(EXPECTED_TEXT, request.getTextToAnalyze());

        assertEquals(EXPECTED_MODEL_VERSION, request.getModelVersion());
        assertEquals(EXPECTED_CONTENT_TYPE, request.getContentType());
        assertEquals(EXPECTED_LANGUAGE, request.getLanguage());
    }

    private AnalyticsModel createModelWithExpectedValues() {
        AnalyticsModel model = mock(AnalyticsModel.class);
        when(model.getContentTypeName()).thenReturn(EXPECTED_CONTENT_TYPE);
        when(model.getImmutableId()).thenReturn(EXPECTED_ID);
        when(model.getTextToAnalyze()).thenReturn(EXPECTED_TEXT);

        return model;
    }
}