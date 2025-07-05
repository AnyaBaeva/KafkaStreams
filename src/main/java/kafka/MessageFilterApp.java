package kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageFilterApp {

  private static final String BLOCKED_USERS_TOPIC = "blocked-users";
  private static final String FORBIDDEN_WORDS_TOPIC = "forbidden-words";
  private static final String FILTERED_MESSAGES_TOPIC = "filtered-messages";
  private static final String MESSAGES_TOPIC = "messages";
  private static final String BLOCKED_USERS_STORE = "blocked-users-store";
  private static final String FORBIDDEN_WORDS_STORE = "forbidden-words-store";
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageFilterApp.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) {
    try {
      FileUtils.deleteDirectory(new File("kafka-streams-state")); // Ваша явная директория
      FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + "/kafka-streams"));
    } catch (IOException e) {
      LOGGER.warn("Failed to clean state directories: {}", e.getMessage());
    }

    KafkaStreams streams = buildStreamsApplication();
    streams.start();

    // Отправляем тестовые данные
    sendTestData();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static KafkaStreams buildStreamsApplication() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "message-filter-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    props.put(StreamsConfig.STATE_DIR_CONFIG, "kafka-streams-state");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try {
      createTopics();
    } catch (Exception e) {
      LOGGER.error("Ошибка при создании топиков: {}", e.getMessage());
      System.exit(1);
    }

    Topology topology = buildTopology();
    LOGGER.info("Топология:\n{}", topology.describe());

    return new KafkaStreams(topology, props);
  }

  private static Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    GlobalKTable<String, String> blockedUsers = builder.globalTable(
        BLOCKED_USERS_TOPIC,
        Materialized.<String, String>as(Stores.persistentKeyValueStore(BLOCKED_USERS_STORE))
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String())
    );

    builder.globalTable(
        FORBIDDEN_WORDS_TOPIC,
        Materialized.<String, String>as(Stores.persistentKeyValueStore(FORBIDDEN_WORDS_STORE))
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String())
    );

    builder.stream(MESSAGES_TOPIC, Consumed.with(Serdes.String(), new MessageSerdes()))
        .peek((key, value) -> LOGGER.info("Received message: key={}, value={}", key, value))
        // Фильтрация заблокированных пользователей
        .leftJoin(blockedUsers,
            (key, message) -> message.getReceiver() + ":" + key, // Формат: "receiver:sender"
            (message, blockReason) -> {
              if (blockReason != null) {
                return null;
              }
              return message;
            }
        )
        .filter((key, message) -> message != null)
        // Маскирование сообщений
        .transformValues(() -> new MessageFilterProcessor())
        .peek((key, value) -> LOGGER.info("Filtered message: key={}, value={}", key, value))
        // Запись в выходной топик
        .to(FILTERED_MESSAGES_TOPIC, Produced.with(Serdes.String(), new MessageSerdes()));

    return builder.build();
  }

  private static void createTopics() throws Exception {
    Properties adminProps = new Properties();
    adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      Set<String> existingTopics = adminClient.listTopics().names().get();

      createTopicIfNotExists(adminClient, existingTopics, MESSAGES_TOPIC);
      createTopicIfNotExists(adminClient, existingTopics, FILTERED_MESSAGES_TOPIC);
      createTopicIfNotExists(adminClient, existingTopics, BLOCKED_USERS_TOPIC);
      createTopicIfNotExists(adminClient, existingTopics, FORBIDDEN_WORDS_TOPIC);
    }
  }

  private static void createTopicIfNotExists(AdminClient adminClient, Set<String> existingTopics,
      String topicName)
      throws Exception {
    if (!existingTopics.contains(topicName)) {
      NewTopic topic = new NewTopic(topicName, 1, (short) 1);
      adminClient.createTopics(Collections.singletonList(topic)).all().get();
      LOGGER.info("Топик {} создан", topicName);
    }
  }

  private static void sendTestData() {
    sendBlockedUsersData();
    sendForbiddenWordsData();
    sendMessagesData();
  }

  private static void sendBlockedUsersData() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(
          new ProducerRecord<>(BLOCKED_USERS_TOPIC, "user1:user2", "blocked")); //blocked by user-1
      producer.send(
          new ProducerRecord<>(BLOCKED_USERS_TOPIC, "user1:user3", "blocked")); //blocked by user-1
      producer.send(
          new ProducerRecord<>(BLOCKED_USERS_TOPIC, "user2:user4", "blocked")); //blocked by user-2
      producer.flush();
      LOGGER.info("Данные о заблокированных пользователях отправлены");
    }
  }

  private static void sendForbiddenWordsData() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(FORBIDDEN_WORDS_TOPIC, "black", "yes"));
      producer.send(new ProducerRecord<>(FORBIDDEN_WORDS_TOPIC, "red", "yes"));
      producer.send(new ProducerRecord<>(FORBIDDEN_WORDS_TOPIC, "gray", "yes"));
      producer.send(new ProducerRecord<>(FORBIDDEN_WORDS_TOPIC, "blue", "no"));
      producer.flush();
      LOGGER.info("Данные о запрещенных словах отправлены");
    }
  }

  private static void sendMessagesData() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    try (Producer<String, String> producer = new KafkaProducer<>(props)) {
      sendMessage(producer, "user4", new Message("blue", "user1"));
      sendMessage(producer, "user2", new Message("yellow", "user1"));
      sendMessage(producer, "user3", new Message("red", "user1"));
      sendMessage(producer, "user5", new Message("black", "user1"));

      producer.flush();
      System.out.println("Все сообщения успешно отправлены");
    } catch (Exception e) {
      System.err.println("Ошибка при отправке сообщений: " + e.getMessage());
    }
  }

  private static void sendMessage(Producer<String, String> producer,
      String sender, Message message) {
    try {
      String jsonMessage = objectMapper.writeValueAsString(message);
      producer.send(new ProducerRecord<>(MESSAGES_TOPIC, sender, jsonMessage),
          (metadata, exception) -> {
            if (exception != null) {
              System.err.println("Ошибка при отправке: " + exception.getMessage());
            } else {
              System.out.printf("Отправлено: key=%s, value=%s, partition=%d, offset=%d%n",
                  sender, jsonMessage, metadata.partition(), metadata.offset());
            }
          });
    } catch (Exception e) {
      System.err.println("Ошибка сериализации сообщения: " + e.getMessage());
    }
  }
}