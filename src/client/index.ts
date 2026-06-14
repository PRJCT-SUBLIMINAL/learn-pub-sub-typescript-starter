import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType } from "../internal/pubsub/bind.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { handlerPause, handlerMove } from "./handlers.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { handlePause } from "../internal/gamelogic/pause.js";

async function main() {
  console.log("Starting Peril client...");
  const CONNECTION_STRING: string = "amqp://guest:guest@localhost:5672/";
  const connection = await amqp.connect(CONNECTION_STRING);
  if (!connection) {
    throw new Error("Cannot establish connection");
  } else {
    console.log("Connection successful!");
  }

  const username = await clientWelcome();

  const gameState = new GameState(username);
  const ch = await connection.createConfirmChannel();

  await subscribeJSON(connection, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState));
  await subscribeJSON(connection, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, `${ArmyMovesPrefix}.*`, SimpleQueueType.Transient, handlerMove(gameState));

  while (true) {
    const input = await getInput();
    if (input.length === 0) continue;

    if (input[0] === "spawn") {
      try {
        commandSpawn(gameState, input);
      } catch {
        console.log("Can't spawn");
      }
      
    } else if (input[0] === "move") {
      try {
        const move = commandMove(gameState, input);
        await publishJSON(ch, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, move);
        console.log("Move published successfully!");
      } catch {
        console.log("Can't move");
      }
      
    } else if (input[0] === "status") {
      try {
        commandStatus(gameState);
      } catch {
        console.log("Unable to fetch status");
      }
      
    } else if (input[0] === "help") {
      try {
        printClientHelp();
      } catch {
        console.log("Unable to print help.")
      }
      
    } else if (input[0] === "spam") {
      try {
        //
      } catch {
        console.log("Spamming not allowed yet!");
      }
      
    } else if (input[0] === "quit") {
      printQuit();
      process.exit(0);
    } else {
      console.log("Command not found.")
      continue;
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
