package kafka;//package kafka;
//import org.apache.kafka.streams.KeyValue;
//import org.apache.kafka.streams.processor.Processor;
//import org.apache.kafka.streams.processor.ProcessorContext;
//import org.apache.kafka.streams.state.KeyValueIterator;
//import org.apache.kafka.streams.state.KeyValueStore;
//
//import java.util.regex.Pattern;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class MessageFilterProcessor implements Processor<String, Message> {
//  private ProcessorContext context;
//  private KeyValueStore<String, String> blockedUsersStore;
//  private KeyValueStore<String, String> forbiddenWordsStore;
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(MessageFilterProcessor.class);
//  @Override
//  public void init(ProcessorContext context) {
//    this.context = context;
//    this.blockedUsersStore = context.getStateStore("blocked-users-store");
//    this.forbiddenWordsStore = context.getStateStore("forbidden-words-store");
//  }
//
//  @Override
//  public void process(String userId, Message message) {
//    // 1. Проверяем, не заблокирован ли пользователь
//    String blockedUser = blockedUsersStore.get(message.getUser());
//    if (blockedUser != null && blockedUser.contains(userId)) {
//      LOGGER.info("Blocked message from user: {}", userId);
//      context.commit();
//      return; // Пропускаем сообщение
//    }
//
//    // 2. Применяем цензуру к запрещенным словам
//    Message censoredMessage = censorMessage(message);
//
//    // 3. Передаем сообщение дальше
//    context.forward(userId, censoredMessage);
//  }
//
//  private Message censorMessage(Message message) {
//    if (message == null) {
//      return message;
//    }
//
//    // Получаем все запрещенные слова
//    KeyValueIterator<String, String> allWords = forbiddenWordsStore.all();
//    while (allWords.hasNext()) {
//      KeyValue<String, String> entry = allWords.next();
//      String forbiddenWord = entry.key;
//      if ("yes".contains(entry.value)) {
//        // Заменяем запрещенное слово на звездочки
//        message.setContent(message.getContent().replaceAll("(?i)" + Pattern.quote(forbiddenWord),
//            "*".repeat(forbiddenWord.length())));
//      }
//    }
//    allWords.close();
//
//    return message;
//  }
//
//  @Override
//  public void close() {}
//}

import java.util.regex.Pattern;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class MessageFilterProcessor implements ValueTransformer<Message, Message> {

  private KeyValueStore<String, String> forbiddenWordsStore;

  @Override
  public void init(ProcessorContext context) {
    this.forbiddenWordsStore = context.getStateStore("forbidden-words-store");
  }

  @Override
  public Message transform(Message message) {
    if (message == null || message.getText() == null) {
      return message;
    }

    // Создаем копию контента для модификации
    String censoredContent = message.getText();

    // Получаем все запрещенные слова из хранилища
    try (KeyValueIterator<String, String> iter = forbiddenWordsStore.all()) {
      while (iter.hasNext()) {

        KeyValue<String, String> entry = iter.next();

        String wordValue = getStringValue(entry.value);
        if ("yes".equals(wordValue)) {
          censoredContent = censoredContent.replaceAll(
              "(?i)" + Pattern.quote(entry.key),
              "*".repeat(entry.key.length())
          );
        }
      }
    }

    return new Message(censoredContent, message.getReceiver());
  }

  private String getStringValue(Object value) {
    if (value == null) return null;
    if (value instanceof ValueAndTimestamp) {
      return ((ValueAndTimestamp<?>) value).value().toString();
    }
    return value.toString();
  }

  @Override
  public void close() {
  }
}