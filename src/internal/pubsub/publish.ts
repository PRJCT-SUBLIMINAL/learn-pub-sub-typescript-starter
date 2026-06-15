import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

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

export async function publishMessagePack<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T
): Promise<void> {
    const vString = encode(value);
    const vStringBuffer = Buffer.from(vString);
    ch.publish(exchange, routingKey, vStringBuffer, { contentType: "application/x-msgpack" });
}