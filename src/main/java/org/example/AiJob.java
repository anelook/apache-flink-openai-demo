package org.example;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.kafka.common.protocol.types.Field;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class AiJob {
    public static void main(String[] args) throws Exception {
        // create Apache Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create an input stream
        DataStream<String> testStream = env.fromElements("cat, mouse, computer", "squirrel");

        // create a processing stream
        DataStream<String> resultDataStream = AsyncDataStream.unorderedWait(
                testStream, new AsyncHttpRequestFunction(), 10000, TimeUnit.MILLISECONDS, 100);

        // output data
        resultDataStream.print();

        // execute the job
        env.execute("Flink plus OpenAI");
    }

    public static  class  AsyncHttpRequestFunction implements AsyncFunction<String, String> {

        ObjectMapper objectMapper = new ObjectMapper();
        @Override
        public void asyncInvoke (String input, ResultFuture<String> resultFuture) {
            // OpenAI API endpoint for text completion
            String apiUrl = "https://api.openai.com/v1/chat/completions";

            // OpenAI API key
            String openAIKey = "your-open-ai-key";
            try {
                // Create a URL object
                URL url = new URL(apiUrl);

                // Open a connection to the URL
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Authorization", "Bearer " + openAIKey);
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setDoOutput(true);

                String payloadString = generatePrompt(input);

                // Write the request payload to the connection
                try (DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream())) {
                    outputStream.writeBytes(payloadString);
                    outputStream.flush();
                } catch (Exception e) {
                    System.out.println("error: " + e.getMessage());
                }

                // Build the response
                StringBuilder response = new StringBuilder();
                try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                }

                // Disconnect the connection
                connection.disconnect();
                JsonNode rootNode = objectMapper.readTree(response.toString());
                // Process response from OpenAI assuming existing response body
                String responseText = rootNode.get("choices").get(0).get("message").get("content").asText();
                // Create a new message object based on response data
                resultFuture.complete(Collections.singleton(responseText));
            } catch (Exception e) {
                // Handle exceptions
                System.out.println("error" + e.getMessage());
            }
        }

        private static String generatePrompt(String input) throws JsonProcessingException {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode payload = mapper.createObjectNode();
            payload.put("model", "gpt-3.5-turbo");

            ArrayNode messageArray = mapper.createArrayNode();

            ObjectNode systemMessage = mapper.createObjectNode();
            systemMessage.put("role", "system");
            systemMessage.put("content", "You're a very smart and funny AI bot!");
            messageArray.add(systemMessage);


            ObjectNode userMessageObject = mapper.createObjectNode();
            userMessageObject.put("role", "user");
            userMessageObject.put("content", "Tell me a joke about: " + input);
            messageArray.add(userMessageObject);

            payload.set("messages", messageArray);

            return mapper.writeValueAsString(payload);
        }
    }
}