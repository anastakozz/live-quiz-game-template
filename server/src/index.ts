import 'dotenv/config';

import { randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket, type RawData } from 'ws';

import type { Game, RegData, User, WSMessage } from './types';

const PORT = Number.parseInt(process.env.PORT ?? '3000', 10);

const wss = new WebSocketServer({ port: PORT });

const usersByName = new Map<string, User>();
const usersById = new Map<string, User>();
const userIdBySocket = new Map<WebSocket, string>();
const socketByUserId = new Map<string, WebSocket>();
const gamesById = new Map<string, Game>();
const gameIdByCode = new Map<string, string>();
const gameIdByUserId = new Map<string, string>();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === 'string' && value.trim().length > 0;

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

const sendRegResponse = (
  ws: WebSocket,
  data: { name: string; index: string; error: boolean; errorText: string },
): void => {
  send(ws, 'reg', data);
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

const bindUserToSocket = (user: User, ws: WebSocket): void => {
  unlinkSocket(ws);

  const oldSocket = socketByUserId.get(user.index);
  if (oldSocket && oldSocket !== ws) {
    userIdBySocket.delete(oldSocket);
  }

  socketByUserId.set(user.index, ws);
  userIdBySocket.set(ws, user.index);
  user.ws = ws;
};

const handleReg = (ws: WebSocket, data: unknown): void => {
  if (!isRecord(data)) {
    sendRegResponse(ws, {
      name: '',
      index: '',
      error: true,
      errorText: 'Invalid reg payload.',
    });
    return;
  }

  const payload = data as Partial<RegData>;
  const name = isNonEmptyString(payload.name) ? payload.name.trim() : '';
  const password = isNonEmptyString(payload.password) ? payload.password.trim() : '';

  if (!name || !password) {
    sendRegResponse(ws, {
      name,
      index: '',
      error: true,
      errorText: 'Name and password are required.',
    });
    return;
  }

  const existingUser = usersByName.get(name);
  if (existingUser) {
    if (existingUser.password !== password) {
      sendRegResponse(ws, {
        name,
        index: existingUser.index,
        error: true,
        errorText: 'Invalid password.',
      });
      return;
    }

    bindUserToSocket(existingUser, ws);
    sendRegResponse(ws, {
      name: existingUser.name,
      index: existingUser.index,
      error: false,
      errorText: '',
    });
    return;
  }

  const user: User = {
    name,
    password,
    index: randomUUID(),
  };

  usersByName.set(user.name, user);
  usersById.set(user.index, user);
  bindUserToSocket(user, ws);

  sendRegResponse(ws, {
    name: user.name,
    index: user.index,
    error: false,
    errorText: '',
  });
};

const handleCreateGame = (ws: WebSocket): void => {
  sendError(ws, 'create_game handler is not implemented yet.');
};

const handleJoinGame = (ws: WebSocket): void => {
  sendError(ws, 'join_game handler is not implemented yet.');
};

const handleStartGame = (ws: WebSocket): void => {
  sendError(ws, 'start_game handler is not implemented yet.');
};

const handleAnswer = (ws: WebSocket): void => {
  sendError(ws, 'answer handler is not implemented yet.');
};

const unlinkSocket = (ws: WebSocket): void => {
  const userId = userIdBySocket.get(ws);
  if (!userId) {
    return;
  }

  userIdBySocket.delete(ws);

  if (socketByUserId.get(userId) === ws) {
    socketByUserId.delete(userId);
  }

  const user = usersById.get(userId);
  if (user?.ws === ws) {
    user.ws = undefined;
  }
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
        handleReg(ws, message.data);
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
    unlinkSocket(ws);
  });
});

console.log(`WebSocket server is running on ws://localhost:${PORT}`);

export { send, broadcast, broadcastPlayers, parseIncomingMessage };
