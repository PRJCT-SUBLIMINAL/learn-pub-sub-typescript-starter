import amqp from "amqplib";
import { declareAndBind, type SimpleQueueType } from "./bind.js";
import { decode } from "@msgpack/msgpack";

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard
}

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType
): Promise<void> {
    const [channel, queueResult] = await declareAndBind(conn, exchange, queueName, key, queueType);

    await channel.consume(queueResult.queue, async (message: amqp.ConsumeMessage | null)=> {
        if (!message) return;

        const messageString = message.content.toString();
        const messageContent = JSON.parse(messageString);

        const ackType = await handler(messageContent);

        // Depending on acknowledge type from enum AckType
        switch(ackType) {
            case AckType.Ack:
                channel.ack(message);
                console.log(`Acknowledged`);
                break;
            
            case AckType.NackRequeue:
                channel.nack(message, false, true);
                console.log(`Not Acknowledged, Requeue`);
                break;
            
            case AckType.NackDiscard:
                channel.nack(message, false, false);
                console.log(`Not Acknowledged, Discard`);
                break;
        };

    });
}

export async function subscribeMessagePack<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType
): Promise<void> {
    const [channel, queueResult] = await declareAndBind(conn, exchange, queueName, key, queueType);

    await channel.consume(queueResult.queue, async (message: amqp.ConsumeMessage | null) => {
        if (!message) return;

        const messageString = message.content;
        const messageContent = decode(messageString) as T;

        const ackType = await handler(messageContent);

        // Depending on acknowledge type from enum AckType
        switch(ackType) {
            case AckType.Ack:
                channel.ack(message);
                console.log(`Acknowledged`);
                break;
            
            case AckType.NackRequeue:
                channel.nack(message, false, true);
                console.log(`Not Acknowledged, Requeue`);
                break;
            
            case AckType.NackDiscard:
                channel.nack(message, false, false);
                console.log(`Not Acknowledged, Discard`);
                break;
        };
    });
}