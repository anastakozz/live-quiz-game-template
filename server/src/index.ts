import 'dotenv/config';

import { randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket, type RawData } from 'ws';

import type { CreateGameData, Game, JoinGameData, Player, Question, RegData, User, WSMessage } from './types';

const PORT = Number.parseInt(process.env.PORT ?? '3000', 10);
const ROOM_CODE_LENGTH = 6;
const ROOM_CODE_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

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

const getUserBySocket = (ws: WebSocket): User | null => {
  const userId = userIdBySocket.get(ws);
  if (!userId) {
    return null;
  }

  return usersById.get(userId) ?? null;
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

const generateRoomCode = (): string => {
  let code = '';

  do {
    code = '';
    for (let i = 0; i < ROOM_CODE_LENGTH; i += 1) {
      const index = Math.floor(Math.random() * ROOM_CODE_ALPHABET.length);
      code += ROOM_CODE_ALPHABET[index];
    }
  } while (gameIdByCode.has(code));

  return code;
};

const sanitizeQuestion = (candidate: unknown): Question | null => {
  if (!isRecord(candidate)) {
    return null;
  }

  const { text, options, correctIndex, timeLimitSec } = candidate;

  if (!isNonEmptyString(text)) {
    return null;
  }

  if (!Array.isArray(options) || options.length !== 4) {
    return null;
  }

  const normalizedOptions = options.map((option) =>
    typeof option === 'string' ? option.trim() : '',
  );
  if (normalizedOptions.some((option) => option.length === 0)) {
    return null;
  }

  if (typeof correctIndex !== 'number' || !Number.isInteger(correctIndex) || correctIndex < 0 || correctIndex > 3) {
    return null;
  }

  if (typeof timeLimitSec !== 'number' || !Number.isFinite(timeLimitSec) || timeLimitSec <= 0) {
    return null;
  }

  return {
    text: text.trim(),
    options: normalizedOptions,
    correctIndex,
    timeLimitSec,
  };
};

const sanitizeQuestions = (data: unknown): Question[] | null => {
  if (!isRecord(data)) {
    return null;
  }

  const payload = data as Partial<CreateGameData>;
  if (!Array.isArray(payload.questions) || payload.questions.length === 0) {
    return null;
  }

  const result: Question[] = [];
  for (const question of payload.questions) {
    const normalized = sanitizeQuestion(question);
    if (!normalized) {
      return null;
    }
    result.push(normalized);
  }

  return result;
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

const handleCreateGame = (ws: WebSocket, data: unknown): void => {
  const user = getUserBySocket(ws);
  if (!user) {
    sendError(ws, 'Please register/login first.');
    return;
  }

  const existingGameId = gameIdByUserId.get(user.index);
  if (existingGameId && gamesById.has(existingGameId)) {
    sendError(ws, 'User is already in an active game.');
    return;
  }

  const questions = sanitizeQuestions(data);
  if (!questions) {
    sendError(ws, 'Invalid questions payload.');
    return;
  }

  const game: Game = {
    id: randomUUID(),
    code: generateRoomCode(),
    hostId: user.index,
    questions,
    players: [],
    currentQuestion: -1,
    status: 'waiting',
    playerAnswers: new Map(),
  };

  gamesById.set(game.id, game);
  gameIdByCode.set(game.code, game.id);
  gameIdByUserId.set(user.index, game.id);

  send(ws, 'game_created', {
    gameId: game.id,
    code: game.code,
  });
};

const handleJoinGame = (ws: WebSocket, data: unknown): void => {
  const user = getUserBySocket(ws);
  if (!user) {
    sendError(ws, 'Please register/login first.');
    return;
  }

  if (!isRecord(data)) {
    sendError(ws, 'Invalid join_game payload.');
    return;
  }

  const payload = data as Partial<JoinGameData>;
  const normalizedCode = isNonEmptyString(payload.code) ? payload.code.trim().toUpperCase() : '';
  if (!normalizedCode) {
    sendError(ws, 'Room code is required.');
    return;
  }

  const gameId = gameIdByCode.get(normalizedCode);
  if (!gameId) {
    sendError(ws, 'Game not found for this room code.');
    return;
  }

  const game = gamesById.get(gameId);
  if (!game) {
    sendError(ws, 'Game not found.');
    return;
  }

  if (game.status !== 'waiting') {
    sendError(ws, 'Game has already started or finished.');
    return;
  }

  if (game.hostId === user.index) {
    sendError(ws, 'Host cannot join their own game as player.');
    return;
  }

  const existingGameId = gameIdByUserId.get(user.index);
  if (existingGameId && existingGameId !== game.id && gamesById.has(existingGameId)) {
    sendError(ws, 'User is already in an active game.');
    return;
  }

  const existingPlayer = game.players.find((player) => player.index === user.index);
  if (existingPlayer) {
    existingPlayer.ws = ws;
    send(ws, 'game_joined', {
      gameId: game.id,
    });
    return;
  }

  const player: Player = {
    name: user.name,
    index: user.index,
    score: 0,
    ws,
  };

  game.players.push(player);
  gameIdByUserId.set(user.index, game.id);

  send(ws, 'game_joined', {
    gameId: game.id,
  });

  broadcast(game, 'player_joined', {
    playerName: player.name,
    playerCount: game.players.length,
  });

  broadcastPlayers(game);
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
        handleCreateGame(ws, message.data);
        break;
      case 'join_game':
        handleJoinGame(ws, message.data);
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
