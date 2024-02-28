package org.example.service.usecase1;

import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.example.service.usecase1.model.GcBasic;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
public class QueryStoreAdapter {

  private final StreamsBuilderFactoryBean factoryBean;
  private final OkHttpClient client = new OkHttpClient();


  @GetMapping("/store/gcBasic/{key}")
  public String getTable(@PathVariable String key) {

    log.info("-- gcBasic store 조회 - key: {} ", key);

    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    ReadOnlyKeyValueStore<String, GcBasic> gcBasicStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType(
            "usecase1-gc-basic-reduce",
            QueryableStoreTypes.keyValueStore()
        )
    );

    GcBasic gcBasic = gcBasicStore.get(key);
    if (gcBasic != null) {
      log.info("-- valid store - key: {}, value: {} ", key, gcBasic.toString());
      return gcBasic.toString();

    } else {
      KeyQueryMetadata keyQueryMetadata = kafkaStreams.queryMetadataForKey("usecase1-gc-basic-reduce", key, Serdes.String().serializer());
      HostInfo hostInfo = keyQueryMetadata.activeHost();

      log.warn("-- invalid store - key : {}, active host : {}, active port: {} ", key, hostInfo.host(), hostInfo.port());
      return fetchByKey(hostInfo, key).toString();
    }
  }

  private Object fetchByKey(final HostInfo hostInfo, final String key) {

    String url = String.format("http://%s:8080/store/gcBasic/%s", hostInfo.host(), key);
    log.info("-- fetchByKey! HostInfo: {}, key: {}, url: {}", hostInfo.toString(), key, url);

    Request request = new Request.Builder().url(url).build();
    try {
      Response response = client.newCall(request).execute();
      return response.peekBody(Long.MAX_VALUE).string();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

}