import type { ConfirmChannel } from "amqplib";

export async function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T
): Promise<void> {
    const vString = JSON.stringify(value);
    const vStringBuffer = Buffer.from(vString);
    ch.publish(exchange, routingKey, vStringBuffer, { contentType: "application/json" });
}