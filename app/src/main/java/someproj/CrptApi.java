package main.java.someproj;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;

import main.java.someproj.CrptApi.Common.ProductGroup;
import main.java.someproj.CrptApi.Common.SendDocsManager;
import main.java.someproj.CrptApi.LpIntroduceGoods.Models.Document;

/**
 * Т.к. в задании есть требование, что количество доступов к api должно быть
 * ограничено в единицу времени, то данный класс реализован в виде singleton.
 * Т.е. все запросы должны проходить через единый объект.
 * Для инициализации первоначальных параметров и создания instance сначала
 * должен быть запущен метод setInstance().
 * 
 */
public class CrptApi {
  private static volatile CrptApi instance;
  private final CrptHttpClient client;
  private static final Moshi moshi = new Moshi.Builder().build();
  private static AtomicInteger counter = new AtomicInteger(0);

  public static CrptApi getInstance() {
    return instance;
  }

  /**
   * Инициализация параметров, создание singleton instance.
   */
  public static synchronized boolean setInstance(TimeUnit timeUnit, int requestLimit) {
    boolean isSuccessful = false;
    if (instance != null)
      return isSuccessful;

    instance = new CrptApi(timeUnit, requestLimit);
    CrptLogger.instance
        .info("Crpt api instance created with params: timeUnit: " + timeUnit + " requestLimit: " + requestLimit);

    isSuccessful = true;
    return isSuccessful;
  }

  private CrptApi(TimeUnit timeUnit, int requestLimit) {
    this.client = new CrptHttpClient(timeUnit, requestLimit);
  }

  /**
   * Это метод, который требуется реализовать в задании:
   * "Создание документа для ввода в оборот товара, произведенного в РФ".
   */
  public CrptResult fetchDocLpIntroduceGoods(LpIntroduceGoods.InputObj inputObj, String signature) {
    try {
      String json = LpIntroduceGoods.moshiAdapter.toJson(inputObj.document);
      CrptResult result = SendDocsManager.send(inputObj.productGroup, json, signature, "LP_INTRODUCE_GOODS");
      return result;
    } catch (Throwable throwable) {
      CrptLogger.instance.error(throwable.getMessage());
      return new CrptResult(throwable);
    }
  }

  /**
   * Общие сущности для разных методов.
   * Например, общие справочники.
   */
  public static class Common {
    private static enum UriConfig {
      DOC_CREATE("/api/v3/lk/documents/create");

      public final String uri;

      UriConfig(String uri) {
        this.uri = uri;
      }
    }

    public static enum ProductGroup {
      clothes(1), shoes(2), tobacco(3),
      perfumery(4), tires(5), electronics(6),
      pharma(7), milk(8), bicycle(9),
      wheelchairs(10);

      public final int code;

      ProductGroup(int code) {
        this.code = code;
      }
    }

    /**
     * Формирование запроса для работы с методами api по созданию документов.
     */
    protected static class SendDocsManager {
      private static JsonAdapter<RequestBodyModel> reqBodyMoshiAdapter = moshi.adapter(RequestBodyModel.class);

      public static CrptResult send(ProductGroup productGroup, String product_document, String signature, String type)
          throws URISyntaxException {
        int requestInfo = getCounter(); // Счётчик просто для целей удобного логирования.
        CrptLogger.instance.info("Crpt request " + requestInfo + " received.");

        String uriStr = CrptHttpClient.getDummyHost()
            + Common.UriConfig.DOC_CREATE.uri
            + "?pg=" + productGroup;

        String signatureBase64 = Base64.getEncoder().encodeToString(signature.getBytes());
        String productDocumentBase64 = Base64.getEncoder().encodeToString(product_document.getBytes());
        RequestBodyModel reqBody = new RequestBodyModel(productDocumentBase64, signatureBase64, type);
        String bodyStr = reqBodyMoshiAdapter.toJson(reqBody);

        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder(new URI(uriStr))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(bodyStr));

        CrptResult result = CrptApi.getInstance().client.send(reqBuilder);
        CrptLogger.instance.info(
            "Crpt request " + requestInfo + " ended with result: ok = " + result.ok + " status = " + result.status);
        return result;
      }

      public static class RequestBodyModel {
        final String document_format = "MANUAL";
        final String product_document;
        final String signature;
        final String type;

        public RequestBodyModel(String product_document, String signature, String type) {
          this.product_document = product_document;
          this.signature = signature;
          this.type = type;
        }
      }
    }
  }

  /**
   * Далее отдельные классы для сущностей специфичных для конкретного метода.
   * Например в данном классе - LpIntroduceGoods, сохранены модели полей и
   * прочие сущности для ввода в оборот товаров, произведенных на территории РФ.
   */
  public static class LpIntroduceGoods {
    private static JsonAdapter<Models.Document> moshiAdapter = moshi.adapter(Models.Document.class);

    public static class InputObj {
      final ProductGroup productGroup;
      final Models.Document document;

      public InputObj(ProductGroup productGroup, Document document) {
        this.productGroup = productGroup;
        this.document = document;
      }
    }

    public static class Models {
      enum ProductionType {
        OWN_PRODUCTION, // Собственное производство
        CONTRACT_PRODUCTION // Производство товара по договору
      }

      enum CertificateDocument {
        CONFORMITY_CERTIFICATE, // сертификат соответствия
        CONFORMITY_DECLARATION // декларация соответствия
      }

      public static class Document {
        final Description description; // 1
        final String doc_id; // 3 Идентификатор
        final String doc_status; // 4 Статус документа
        final String doc_type; // 5 Тип документа
        final String importRequest; // 6
        final String owner_inn; // 7 ИНН собственника товара
        final String participant_inn; // 8 ИНН участника оборота товаров
        final String producer_inn; // 9 ИНН производителя товара
        final String production_date; // 10 Дата производства товара
        final ProductionType production_type; // 11 Тип
        final List<Product> products; // 12 Перечень товаров
        final String reg_date; // 22 Дата и время регистрации
        final String reg_number; // 23 Регистрационный номер документа

        public Document(Description description,
            String doc_id, String doc_status, String doc_type, String importRequest,
            String owner_inn, String participant_inn, String producer_inn, String production_date,
            ProductionType production_type,
            List<Product> products, String reg_date, String reg_number) {
          this.description = description;
          this.doc_id = Objects.requireNonNull(doc_id);
          this.doc_status = Objects.requireNonNull(doc_status);
          this.doc_type = Objects.requireNonNull(doc_type);
          this.importRequest = importRequest;
          this.owner_inn = Objects.requireNonNull(owner_inn);
          this.participant_inn = Objects.requireNonNull(participant_inn);
          this.producer_inn = Objects.requireNonNull(producer_inn);
          this.production_date = Objects.requireNonNull(production_date);
          Asserts.isoLocalDate(production_date);
          this.production_type = Objects.requireNonNull(production_type);
          this.products = products;
          this.reg_date = Objects.requireNonNull(reg_date);
          try {
            Asserts.isoLocalDateTime(reg_date);
          } catch (Exception e) {
            Asserts.isoLocalDate(reg_date);
          }
          this.reg_number = reg_number;
        }
      }

      public static class Product {
        final CertificateDocument certificate_document; // 13 Код вида документа обязательной
                                                        // сертификации
        final String certificate_document_date; // 14 Дата документа обязательной сертификации
        final String certificate_document_number; // 15 Номер документа обязательной сертификации
        final String owner_inn; // 16 ИНН собственника
        final String producer_inn; // 17 ИНН производителя товара
        final String production_date; // 18 Дата производства товара из общих сведениях о вводе товаров в оборот
        final String tnved_code; // 19 Код товарной номенклатуры (10 знаков)
        final String uit_code; // 20 Уникальный идентификатор товара
        final String uitu_code; // 21 Уникальный идентификатор транспортной упаковки

        public Product(CertificateDocument certificate_document,
            String certificate_document_date, String certificate_document_number,
            String owner_inn, String producer_inn,
            String production_date, String tnved_code,
            String uit_code, String uitu_code) {
          this.certificate_document = certificate_document;
          this.certificate_document_date = Asserts.isoLocalDate(certificate_document_date);
          this.certificate_document_number = certificate_document_number;
          this.owner_inn = Objects.requireNonNull(owner_inn);
          this.producer_inn = Objects.requireNonNull(producer_inn);
          this.production_date = production_date;
          Asserts.isoLocalDate(production_date);
          this.tnved_code = Objects.requireNonNull(tnved_code);
          this.uit_code = uit_code;
          this.uitu_code = uitu_code;
          if (uit_code == null && uitu_code == null)
            throw new NullPointerException("uit_code == null && uitu_code == null");
        }
      }

      public static class Description {
        String participantInn; // 2 ИНН Участника оборота товаров

        public Description(String participantInn) {
          this.participantInn = Objects.requireNonNull(participantInn);
        }
      }
    }
  }

  /**
   * Счётчик для нумерации запросов просто для более удобного отображения при
   * логировании и отладке.
   * 
   * @return
   */
  private static int getCounter() {
    int counter;
    int newCounter;
    do {
      counter = CrptApi.counter.get();
      newCounter = counter < Integer.MAX_VALUE ? counter + 1 : 1;
    } while (!CrptApi.counter.compareAndSet(counter, newCounter));
    return newCounter;
  }
}

////////////////////////////////////////////////////////////////////////////////
// Далее классы разных используемых утилит.
////////////////////////////////////////////////////////////////////////////////
class CrptLogger {
  public static final LoggerI instance = Logger.getStaticInstance();
}

interface LoggerI {
  public void debug(String message);

  public void info(String message);

  public void warning(String message);

  public void error(String message);
}

class Logger implements LoggerI {
  private static volatile Logger staticInstance;
  private int logLevelNum = LogLevels.INFO.num;

  public Logger() {
  }

  public static Logger getStaticInstance() {
    if (staticInstance == null) {
      synchronized (Logger.class) {
        if (staticInstance == null)
          staticInstance = new Logger();
      }
    }
    return staticInstance;
  }

  public static enum LogLevels {
    DEBUG(10), INFO(20), WARNING(30), ERROR(40);

    public final int num;

    LogLevels(int num) {
      this.num = num;
    }
  }

  private void log(LogLevels logLevel, String message) {
    if (logLevel.num >= this.logLevelNum)
      System.out.println(logLevel + ": " + message);
  }

  @Override
  public void debug(String message) {
    log(LogLevels.DEBUG, message);
  }

  @Override
  public void info(String message) {
    log(LogLevels.INFO, message);
  }

  @Override
  public void warning(String message) {
    log(LogLevels.WARNING, message);
  }

  @Override
  public void error(String message) {
    log(LogLevels.ERROR, message);
  }
}

/**
 * Результат выполнения запроса к api.
 */
class CrptResult {
  public final boolean ok;
  public final Integer status;
  public final String body;
  public final Throwable throwable;

  public CrptResult(boolean ok, Integer status, String body, Throwable throwable) {
    this.ok = ok;
    this.status = status;
    this.body = body;
    this.throwable = throwable;
  }

  public CrptResult(Throwable throwable) {
    this(false, null, null, throwable);
  }

  public CrptResult(Integer status, String body) {
    this(status >= 200 && status <= 299 ? true : false, status, body, null);
  }
}

/**
 * Http клиент запросов к api с rateLimiter - ограничителем по количеству
 * запросов в единицу времени.
 */
class CrptHttpClient {
  private final RateLimiter rateLimiter;
  private final HttpClient client = HttpClient.newHttpClient();

  /**
   * Насколько я понимаю, авторизацию прорабатывать не надо?
   * Сделан dummy метод.
   */
  private static String getDummyClientToken() {
    return "1cecc8fb-fb47-4c8a-af3d-d34c1ead8c4f";
  }

  /**
   * Насколько я понимаю, хранение настроек хостов прорабатывать не надо?
   * Сделан dummy метод.
   * 
   * @return
   */
  protected static String getDummyHost() {
    // http://localhost:8080
    return "https://ismp.crpt.ru";
  }

  protected CrptHttpClient(TimeUnit timeUnit, int requestLimit) {
    this.rateLimiter = new RateLimiter(timeUnit, requestLimit);
  }

  /**
   * Данный метод должен использоваться для отправки запросов к api всеми другими
   * методами.
   */
  protected CrptResult send(HttpRequest.Builder reqBuilder) {
    reqBuilder.header("Authorization", CrptHttpClient.getDummyClientToken());
    HttpRequest request = reqBuilder.build();

    try {
      rateLimiter.acquire();
      HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
      return new CrptResult(response.statusCode(), response.body());
    } catch (IOException | InterruptedException e) {
      return new CrptResult(e);
    }
  }

}

/**
 * В данном классе собраны полезные методы для проверки консистентности
 * полученных данных.
 * Например, что даты указаны в правильных форматах.
 */
class Asserts {
  public static String isoLocalDate(String dateStr) {
    if (dateStr != null)
      DateTimeFormatter.ISO_LOCAL_DATE.parse(dateStr);
    return dateStr;
  }

  public static String isoLocalDateTime(String dateTimeStr) {
    if (dateTimeStr != null)
      DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(dateTimeStr);
    return dateTimeStr;
  }
}

/**
 * Циклический буфер для сохранения времени последних запросов, чтобы
 * ограничивать количество запросов в единицу времени.
 */
class CircularBuffer {
  private final long[] buffer;
  private final int maxIndex;
  private int lastIndex = 0;
  private long lastValue = 0;

  public CircularBuffer(int length) {
    if (length < 1)
      throw new IllegalArgumentException("Circular buffer length < 1.");
    this.buffer = new long[length];
    this.maxIndex = length - 1;
  }

  public synchronized void insert(long value) {
    buffer[lastIndex] = value;
    lastIndex = lastIndex < maxIndex ? lastIndex + 1 : 0;
    lastValue = buffer[lastIndex];
  }

  public long getLast() {
    return lastValue;
  }
}

/**
 * Ограничитель количества запросов в единицу времени.
 */
class RateLimiter {
  private final CircularBuffer requestsHistory;
  private final long timeUnitMs;

  public RateLimiter(TimeUnit timeUnit, int requestLimit) {
    if (requestLimit < 1)
      throw new IllegalArgumentException("Limit of requests < 1.");
    this.timeUnitMs = timeUnit.toMillis(1);
    if (timeUnitMs < 1)
      throw new IllegalArgumentException("Time unit in ms < 1.");
    this.requestsHistory = new CircularBuffer(requestLimit);
  }

  /**
   * Данный метод управляет выдачей разрешений на передачу запросов в api, чтобы
   * не превышать допустимый предел.
   */
  public void acquire() {
    synchronized (requestsHistory) {
      while (true) {
        long oldestRequestTime = requestsHistory.getLast();
        long deltaTime = System.currentTimeMillis() - oldestRequestTime;
        if (deltaTime < timeUnitMs)
          try {
            long sleepTime = timeUnitMs - deltaTime;
            CrptLogger.instance.info("Crpt request processor goes to sleep for " + sleepTime + " ms.");
            Thread.sleep(sleepTime);
            CrptLogger.instance.info("Crpt request processor resumes execution.");
          } catch (InterruptedException e) {
            CrptLogger.instance.info("Crpt request processor was interrupted. Stack trace: ");
            e.printStackTrace();
            continue;
          }
        break;
      }
      requestsHistory.insert(System.currentTimeMillis());
    }
  }
}
