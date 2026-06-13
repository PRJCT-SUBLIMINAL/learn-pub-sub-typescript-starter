import amqp from "amqplib";

async function main() {
  const CONNECTION_STRING: string = "amqp://guest:guest@localhost:5672/";
  const connection = await amqp.connect(CONNECTION_STRING);
  if (!connection) {
    throw new Error("Cannot establish connection");
  } else {
    console.log("Connection successful!");
  }

  process.on("SIGINT", async ()=>{
    console.log("Shutting down.");
    await connection.close();
    process.exit(0);
  })
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
