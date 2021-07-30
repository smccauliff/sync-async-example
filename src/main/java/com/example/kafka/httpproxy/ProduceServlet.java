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

    // Sync means the client wants to wait
    if (callMode.equals("wait-for-ack")) {
      asyncContext = req.startAsync();
      _producer.send(record, new ProduceAcknowledge(asyncContext));
      // Async means the client does not want to wait for produce acknowledgement.
    } else if (callMode.equals("no-wait-for-ack")) {
      _producer.send(record);
      resp.setStatus(HttpStatus.OK_200);
    } else {
      throw new IllegalArgumentException("call-mode must be either wait-for-ack|no-wait-for-ack");
    }

  }
}
