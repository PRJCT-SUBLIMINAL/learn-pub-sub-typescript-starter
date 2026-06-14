import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove } from "../internal/gamelogic/move.js";
import { AckType } from "../internal/pubsub/subscribe.js";


export function handlerPause(gameState: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => {
        const ackType = handlePause(gameState, ps);
        process.stdout.write("> ");
        return ackType;
    }
}

export function handlerMove(gameState: GameState): (am: ArmyMove) => AckType {
    return (am: ArmyMove) => {
        const ackType = handleMove(gameState, am);
        process.stdout.write("> ");
        return ackType;
    }
}