import amqp from "amqplib";
import type { Channel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient
}

export async function declareAndBind(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const ch = await conn.createChannel();

    let isDurable = true;
    let autoDelete = false;
    let isExclusive = false;

    if (queueType === SimpleQueueType.Transient) {
        isDurable = false;
        autoDelete = true;
        isExclusive = true;
    };

    const result = await ch.assertQueue(queueName, { durable: isDurable, autoDelete: autoDelete, exclusive: isExclusive });

    if (!result) throw new Error("Failed to assert queue!");

    await ch.bindQueue(result.queue, exchange, key);

    return [ch, result];
}