import { type Consumer, Kafka, Partitioners, type Producer } from "kafkajs";
import { Logger } from "./logger.ts";

const KAFKA_BROKERS = Deno.env.get("KAFKA_BROKERS")?.split(",") ?? ["127.0.0.1:9092"];

export const kafkaProducer: Kafka = new Kafka({
  clientId: "webhook-producer",
  brokers: KAFKA_BROKERS,
});

export const kafkaConsumer: Kafka = new Kafka({
  clientId: "webhook-consumer",
  brokers: KAFKA_BROKERS,
});

export const kafkaAdmin: Kafka = new Kafka({
  clientId: "webhook-admin",
  brokers: KAFKA_BROKERS,
});

export class PubSub {
  private producer: Producer;
  private consumer: Consumer;
  private logger: Logger;

  constructor(groupId: string) {
    this.logger = new Logger("kafka-pubsub");
    this.producer = kafkaProducer.producer({
      allowAutoTopicCreation: true,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    this.consumer = kafkaConsumer.consumer({ groupId });

    this.producer.on("producer.connect", () => {
      this.logger.info("Kafka Producer connected");
    });
    this.producer.on("producer.disconnect", () => {
      this.logger.info("Kafka Producer disconnected");
    });

    this.consumer.on("consumer.connect", () => {
      this.logger.info("Kafka Consumer connected");
    });
    this.consumer.on("consumer.disconnect", () => {
      this.logger.info("Kafka Consumer disconnected");
    });
  }

  async close() {
    await this.producer.disconnect();
    await this.consumer.disconnect();
  }

  async setupProducer() {
    try {
      await this.producer.connect();
    } catch (e) {
      this.logger.error("Producer connection error", e);
      throw e;
    }
    return this;
  }

  async setupConsumer(topics: string[]) {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({
        topics: topics,
        fromBeginning: true,
      });
    } catch (e) {
      this.logger.error("Consumer connection error", e);
      throw e;
    }
    return this;
  }

  async consume(
    fn: (
      message: string,
      { heartbeat }: { heartbeat: () => Promise<void> },
    ) => Promise<void>,
  ) {
    const partitionsConcurrency = parseInt(Deno.env.get("PARTITIONS_CONCURRENCY") ?? "3", 10);
    const sessionTimeout = parseInt(Deno.env.get("SESSION_TIMEOUT") ?? "90000", 10);

    try {
      await this.consumer.run({
        partitionsConsumedConcurrently: partitionsConcurrency,
        sessionTimeout: sessionTimeout,
        autoCommit: false,
        eachMessage: async ({
          topic,
          partition,
          message,
          heartbeat,
        }) => {
          if (!message?.value?.toString()) {
            this.logger.error("Received message with no payload");
            return;
          }

          try {
            await fn(message?.value?.toString(), { heartbeat });
          } catch (error) {
            this.logger.error("Failed to process message", error);
          } finally {
            await this.consumer.commitOffsets([
              {
                topic,
                partition,
                offset: (Number(message.offset) + 1).toString(),
              },
            ]);
          }
        },
      });
    } catch (e) {
      this.logger.error("Consumer run error", e);
      throw e;
    }
  }

  async sendMessage(topic: string, messages: { key: string; value: string }[]) {
    await this.producer.send({
      topic,
      messages,
      timeout: 30000,
    });
  }
}
