package com.example.kafka.httpproxy;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;
import javax.servlet.AsyncContext;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.jetty.http.HttpStatus;


@WebServlet(urlPatterns={"/produce"}, asyncSupported=true)
public class ProduceServlet extends HttpServlet {


  private final class ProduceAcknowledge implements Callback {
    private final AsyncContext _asyncContext;

    public ProduceAcknowledge(AsyncContext asyncContext) {
      _asyncContext = asyncContext;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      HttpServletResponse httpServletResponse = ((HttpServletResponse) _asyncContext.getResponse());

      if (exception != null) {
        httpServletResponse.setStatus(HttpStatus.IM_A_TEAPOT_418);
      } else {
        try {
          httpServletResponse.getOutputStream().print("offset = " + metadata.offset());
        } catch (IOException e) {
          e.printStackTrace();
        }
        httpServletResponse.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        _asyncContext.complete();
      }
    }
  }

  private final Producer<String, String> _producer;

  public ProduceServlet() {
    Properties producerProperties = new Properties();
    producerProperties.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    producerProperties.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    producerProperties.put("acks", "1");

    producerProperties.put("bootstrap.servers", "localhost:9092");

    _producer = new KafkaProducer<>(producerProperties);
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    AsyncContext asyncContext = null;

    String value = IOUtils.toString(req.getInputStream(), Charset.defaultCharset());
    ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", value);

    String callMode = req.getParameter("call-mode");

    if (callMode.equals("async")) {
      // Paradoxically if we want to do a sync call efficiently we need to have an async context so we don't
      // tie up this thread forever
      asyncContext = req.startAsync();
      _producer.send(record, new ProduceAcknowledge(asyncContext));
    } else if (callMode.equals("sync")) {
      _producer.send(record);
    } else {
      throw new IllegalArgumentException("call-mode must be either sync|async");
    }

  }
}
