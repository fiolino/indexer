package org.fiolino.indexer.analytics;

import static com.pvs.pmm.domainmodel.mandatorconfiguration.platform.PlatformSearchMandatorConfig.PLATFORM_SEARCH_USER_LOGIN;
import static com.pvs.pmm.domainmodel.mandatorconfiguration.platform.PlatformSearchMandatorConfig.PLATFORM_SEARCH_USER_PASSWORD;
import static com.pvs.pmm.domainmodel.mandatorconfiguration.search.SearchConfig.ANALYTICS_API_URL_PREFIX;
import static com.pvs.pmm.domainmodel.mandatorconfiguration.search.SearchConfig.ANALYTICS_UNIQUE_CLIENT_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;

import org.fiolino.indexer.domain.Mandator;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.mls.domain.analytics.model.AnalyticsRequest;
import com.mls.domain.analytics.model.AnalyticsRequestBuilder;
import com.mls.domain.analytics.model.searchDossier.ProductCategory;

public class AnalyticsServiceIntegrationTest {

  public static final String DOMAIN = "https://analytics-service.marketlogicsoftware.com/";

  public static final String TEXT = "Poland • The average number of meals per day is the highest in Poland (weekday: 3.65; weekend: 4.00). On the weekend 2.19 meals per day are eaten with other (highest no.) • The diets “no meat day 1-2x a week” and “only all natural” are practiced the most in Poland (15%; 10%). • Having a busy day is the most important situation for using convenience food (for 63% of respondents). And 30% say “I can feel good about convenient food when I don’t have time.” • The aspect of convenience food the Polish disliked the most is that it contains ingredients like MSG, colorants, preservatives or artificial ingredients. • The percentage of respondents claiming to buy products that are “made with natural ingredients” (72%) and “free from” (51%) is highest in Poland. But the likelihood to buy a product with sustainably sourced ingredients is the lowest.";

  @Test
  @Ignore("Ignore to not break the build if analytics service is down")
  public void concreteWebServiceCall() throws Exception {

    System.out.println("--- Analytics service request");
    System.out.println("Domain : " + DOMAIN);
    System.out.println("Text   : " + TEXT);

    System.out.println();
    System.out.println("Calling...");

    BunchOfAnalyticsData data = new AnalyticsService("v1", "en").fetchData(createMockedMandator(DOMAIN), createConcreteRequest(TEXT));

    System.out.println();
    System.out.println("--- Analytics service response");
    System.out.println(data);

    assertThat(data, is(notNullValue()));
    assertThat(data.getAnalysisMap(), is(notNullValue()));
    assertThat(data.getAnalysisMap().isEmpty(), is(false));
  }

  private Mandator createMockedMandator(String domain) {
    Mandator mandator = Mockito.mock(Mandator.class, RETURNS_DEEP_STUBS);

    when(mandator.getId()).thenReturn(1000000000000L);

    when(mandator.getMandatorDomain().getConfig(ANALYTICS_API_URL_PREFIX)).thenReturn(domain);
    when(mandator.getMandatorDomain().getConfig(ANALYTICS_UNIQUE_CLIENT_ID)).thenReturn("2ee4d708-1a69-40d3-b817-cd1f3daa537f");

    when(mandator.getMandatorDomain().getConfig(PLATFORM_SEARCH_USER_LOGIN)).thenReturn("user");
    when(mandator.getMandatorDomain().getConfig(PLATFORM_SEARCH_USER_PASSWORD)).thenReturn("password");

    return mandator;
  }

  private AnalyticsRequest createConcreteRequest(String text) {
    AnalyticsRequestBuilder builder = new AnalyticsRequestBuilder();
    builder.withModelVersion("v1");
    builder.withLanguage("en");
    builder.withContentType("finding");
    builder.withMandatorId("UNILEVER");
    builder.withDocumentId("TEST-123456789");
    builder.withTextToAnalyze(text);
    builder.withProductCategories(new HashSet<>(Arrays.asList(new ProductCategory("Hair Care", "1372859167819"))));
    builder.withStore(false);

    return builder.build();
  }
}
