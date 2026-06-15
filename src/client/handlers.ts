import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import type { ConfirmChannel } from "amqplib";
import { WarRecognitionsPrefix, ExchangePerilTopic } from "../internal/routing/routing.js";
import { handleWar } from "../internal/gamelogic/war.js";
import { WarOutcome } from "../internal/gamelogic/war.js";
import { publishGameLog } from "./index.js";


export function handlerPause(gameState: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => {
        const ackType = handlePause(gameState, ps);
        process.stdout.write("> ");
        return ackType;
    }
}

export function handlerMove(gameState: GameState, ch: ConfirmChannel): (am: ArmyMove) => Promise<AckType> {
    return async (am: ArmyMove): Promise<AckType> => {
        const moveOutcome = handleMove(gameState, am);
        process.stdout.write("> ");
        
        switch(moveOutcome) {
            case MoveOutcome.MakeWar:
                const rw: RecognitionOfWar = {
                    attacker: am.player,
                    defender: gameState.getPlayerSnap()
                };
                await publishJSON(ch, ExchangePerilTopic,`${WarRecognitionsPrefix}.${gameState.getUsername()}`, rw);

                return AckType.Ack;

            case MoveOutcome.Safe:
                return AckType.Ack;

            case MoveOutcome.SamePlayer:
                return AckType.NackDiscard;

            default:
                return AckType.NackRequeue;
        }
    }
}

export function handlerWar(gameState: GameState, ch: ConfirmChannel): (war: RecognitionOfWar) => Promise<AckType> {
    return async (war: RecognitionOfWar): Promise<AckType> => {
        const outcome = handleWar(gameState, war);

        try {
            switch(outcome.result) {
                case WarOutcome.NotInvolved:
                    return AckType.NackRequeue;

                case WarOutcome.NoUnits:
                    return AckType.NackDiscard;

                case WarOutcome.OpponentWon:
                case WarOutcome.YouWon:
                    const winner = outcome.winner;
                    const loser = outcome.loser;
                    try {
                        await publishGameLog(ch, gameState.getUsername(), `${winner} won a war against ${loser}`);
                        return AckType.Ack;
                    } catch {
                        return AckType.NackRequeue;
                    }
                    

                case WarOutcome.Draw:
                    try {
                        await publishGameLog(ch, gameState.getUsername(), `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`);
                        return AckType.Ack;
                    } catch {
                        return AckType.NackRequeue;
                    }
                    
                default:
                    console.error("Error! No war outcome.");
                    return AckType.NackDiscard;
            }
        } finally {
            process.stdout.write("> ");
        }
        
    }
}