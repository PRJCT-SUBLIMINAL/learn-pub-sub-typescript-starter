import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printClientHelp, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType } from "../internal/pubsub/bind.js";
import { subscribeMessagePack, AckType } from "../internal/pubsub/subscribe.js";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";

async function main() {
  const CONNECTION_STRING: string = "amqp://guest:guest@localhost:5672/";
  const connection = await amqp.connect(CONNECTION_STRING);
  if (!connection) {
    throw new Error("Cannot establish connection");
  } else {
    console.log("Connection successful!");
  }

  const ch = await connection.createConfirmChannel();
  publishJSON(ch, ExchangePerilDirect, PauseKey, { isPaused: true });
  
  await subscribeMessagePack(connection, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable, async (log: GameLog) => {
    await writeLog(log);
    return AckType.Ack;
  })

  process.on("SIGINT", async ()=>{
    console.log("Shutting down.");
    await connection.close();
    process.exit(0);
  })

  // Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }

  printServerHelp();

  while (true) {
    const input = await getInput();
    if (input.length === 0) continue;

    if (input[0] === "pause") {
      console.log("Sending pause message...");
      publishJSON(ch, ExchangePerilDirect, PauseKey, { isPaused: true });
    } else if (input[0] === "resume") {
      console.log("Sending resume message...");
      publishJSON(ch, ExchangePerilDirect, PauseKey, { isPaused: false });
    } else if (input[0] === "quit") {
      console.log("Exiting...");
      process.exit(0);
    } else {
      console.log("Command not found.");
      continue;
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
