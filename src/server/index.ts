import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/bind.js";

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
  
  await declareAndBind(connection, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable);

  process.on("SIGINT", async ()=>{
    console.log("Shutting down.");
    await connection.close();
    process.exit(0);
  })

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
