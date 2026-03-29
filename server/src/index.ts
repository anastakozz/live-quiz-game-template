import 'dotenv/config';

import { WebSocketServer, WebSocket, type RawData } from 'ws';

import type { Game, WSMessage } from './types';

const PORT = Number.parseInt(process.env.PORT ?? '3000', 10);

const wss = new WebSocketServer({ port: PORT });

const socketByUserId = new Map<string, WebSocket>();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const send = (ws: WebSocket, type: string, data: unknown): void => {
  if (ws.readyState !== WebSocket.OPEN) {
    return;
  }

  const payload: WSMessage = {
    type,
    data,
    id: 0,
  };

  ws.send(JSON.stringify(payload));
};

const sendError = (ws: WebSocket, message: string): void => {
  send(ws, 'error', { message });
};

const broadcast = (game: Game, type: string, data: unknown): void => {
  const recipients = new Set<WebSocket>();

  const hostSocket = socketByUserId.get(game.hostId);
  if (hostSocket) {
    recipients.add(hostSocket);
  }

  for (const player of game.players) {
    if (player.ws) {
      recipients.add(player.ws);
    }
  }

  for (const ws of recipients) {
    send(ws, type, data);
  }
};

const broadcastPlayers = (game: Game): void => {
  const payload = game.players.map((player) => ({
    name: player.name,
    index: player.index,
    score: player.score,
  }));

  broadcast(game, 'update_players', payload);
};

const parseIncomingMessage = (rawData: RawData): WSMessage | null => {
  try {
    const rawText = typeof rawData === 'string' ? rawData : rawData.toString();
    const parsed = JSON.parse(rawText) as unknown;

    if (!isRecord(parsed) || typeof parsed.type !== 'string') {
      return null;
    }

    if ('id' in parsed && parsed.id !== 0) {
      return null;
    }

    return {
      type: parsed.type,
      data: parsed.data,
      id: 0,
    };
  } catch {
    return null;
  }
};

const handleReg = (ws: WebSocket): void => {
  sendError(ws, 'reg handler is not implemented yet (Part 1 only).');
};

const handleCreateGame = (ws: WebSocket): void => {
  sendError(ws, 'create_game handler is not implemented yet (Part 1 only).');
};

const handleJoinGame = (ws: WebSocket): void => {
  sendError(ws, 'join_game handler is not implemented yet (Part 1 only).');
};

const handleStartGame = (ws: WebSocket): void => {
  sendError(ws, 'start_game handler is not implemented yet (Part 1 only).');
};

const handleAnswer = (ws: WebSocket): void => {
  sendError(ws, 'answer handler is not implemented yet (Part 1 only).');
};

wss.on('connection', (ws) => {
  ws.on('message', (rawData) => {
    const message = parseIncomingMessage(rawData);
    if (!message) {
      sendError(ws, 'Invalid message format.');
      return;
    }

    switch (message.type) {
      case 'reg':
        handleReg(ws);
        break;
      case 'create_game':
        handleCreateGame(ws);
        break;
      case 'join_game':
        handleJoinGame(ws);
        break;
      case 'start_game':
        handleStartGame(ws);
        break;
      case 'answer':
        handleAnswer(ws);
        break;
      default:
        sendError(ws, `Unknown message type: ${message.type}`);
    }
  });

  ws.on('close', () => {
    for (const [userId, socket] of socketByUserId) {
      if (socket === ws) {
        socketByUserId.delete(userId);
        break;
      }
    }
  });
});

console.log(`WebSocket server is running on ws://localhost:${PORT}`);

export { send, broadcast, broadcastPlayers, parseIncomingMessage };
