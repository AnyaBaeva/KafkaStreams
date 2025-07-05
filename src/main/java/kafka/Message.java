package kafka;
//package kafka;
//
//import java.io.Serializable;
//import java.util.Objects;
//
//public class Message implements Serializable {
//    private String text;
//    private String receiver;  // ID пользователя-получателя
//
//    // Конструктор
//    public Message(String text, String receiver) {
//        this.text = text;
//        this.receiver = receiver;
//    }
//
//    // Геттеры и сеттеры
//    public String getText() {
//        return text;
//    }
//
//    public void setText(String text) {
//        this.text = text;
//    }
//
//    public String getReceiver() {
//        return receiver;
//    }
//
//    public void setReceiver(String receiver) {
//        this.receiver = receiver;
//    }
//
//    // Для корректной работы в Kafka Streams
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        Message message = (Message) o;
//        return Objects.equals(text, message.text) &&
//            Objects.equals(receiver, message.receiver);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(text, receiver);
//    }
//
//    @Override
//    public String toString() {
//        return "Message{" +
//            "text='" + text + '\'' +
//            ", receiver='" + receiver + '\'' +
//            '}';
//    }
//}

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
    private final String text;
    private final String receiver;

    @JsonCreator
    public Message(@JsonProperty("text") String text,
        @JsonProperty("receiver") String receiver) {
        this.text = text;
        this.receiver = receiver;
    }

    // Геттеры
    public String getText() {
        return text;
    }

    public String getReceiver() {
        return receiver;
    }

    @Override
    public String toString() {
        return "Message{" +
            "text='" + text + '\'' +
            ", receiver='" + receiver + '\'' +
            '}';
    }
}