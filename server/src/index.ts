import 'dotenv/config';

import { randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket, type RawData } from 'ws';

import type {
  AnswerData,
  CreateGameData,
  Game,
  JoinGameData,
  Player,
  Question,
  RegData,
  StartGameData,
  User,
  WSMessage,
} from './types';

const PORT = Number.parseInt(process.env.PORT ?? '3000', 10);
const ROOM_CODE_LENGTH = 6;
const ROOM_CODE_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
const BASE_POINTS = 1000;
const NEXT_QUESTION_DELAY_MS = 3000;

const wss = new WebSocketServer({ port: PORT });

const usersByName = new Map<string, User>();
const usersById = new Map<string, User>();
const userIdBySocket = new Map<WebSocket, string>();
const socketByUserId = new Map<string, WebSocket>();
const gamesById = new Map<string, Game>();
const gameIdByCode = new Map<string, string>();
const gameIdByUserId = new Map<string, string>();
const finalizedQuestionKeys = new Set<string>();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === 'string' && value.trim().length > 0;

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

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

const makeQuestionKey = (gameId: string, questionIndex: number): string =>
  `${gameId}:${questionIndex}`;

const allPlayersAnswered = (game: Game): boolean =>
  game.players.length > 0 && game.playerAnswers.size >= game.players.length;

const clearGameTimers = (game: Game): void => {
  if (game.questionTimer) {
    clearTimeout(game.questionTimer);
    game.questionTimer = undefined;
  }

  if (game.transitionTimer) {
    clearTimeout(game.transitionTimer);
    game.transitionTimer = undefined;
  }
};

const cleanupGame = (gameId: string): void => {
  const game = gamesById.get(gameId);
  if (!game) {
    return;
  }

  clearGameTimers(game);

  gameIdByCode.delete(game.code);

  if (gameIdByUserId.get(game.hostId) === game.id) {
    gameIdByUserId.delete(game.hostId);
  }

  for (const player of game.players) {
    if (gameIdByUserId.get(player.index) === game.id) {
      gameIdByUserId.delete(player.index);
    }
  }

  for (const key of [...finalizedQuestionKeys]) {
    if (key.startsWith(`${gameId}:`)) {
      finalizedQuestionKeys.delete(key);
    }
  }

  gamesById.delete(gameId);
};

const sendQuestion = (game: Game, questionIndex: number): void => {
  if (game.status !== 'in_progress') {
    return;
  }

  const question = game.questions[questionIndex];
  if (!question) {
    return;
  }

  clearGameTimers(game);

  game.currentQuestion = questionIndex;
  game.playerAnswers.clear();
  game.questionStartTime = Date.now();

  const questionKey = makeQuestionKey(game.id, questionIndex);
  finalizedQuestionKeys.delete(questionKey);

  game.questionTimer = setTimeout(() => {
    finalizeQuestion(game.id, questionIndex);
  }, question.timeLimitSec * 1000);

  broadcast(game, 'question', {
    questionNumber: questionIndex + 1,
    totalQuestions: game.questions.length,
    text: question.text,
    options: question.options,
    timeLimitSec: question.timeLimitSec,
  });
};

const finishGame = (game: Game): void => {
  game.status = 'finished';
  clearGameTimers(game);

  const scoreboard = [...game.players]
    .sort((a, b) => b.score - a.score || a.name.localeCompare(b.name))
    .map((player, index) => ({
      name: player.name,
      score: player.score,
      rank: index + 1,
    }));

  broadcast(game, 'game_finished', { scoreboard });
  cleanupGame(game.id);
};

const finalizeQuestion = (gameId: string, questionIndex: number): void => {
  const game = gamesById.get(gameId);
  if (!game || game.status !== 'in_progress') {
    return;
  }

  if (game.currentQuestion !== questionIndex) {
    return;
  }

  const question = game.questions[questionIndex];
  if (!question) {
    return;
  }

  const key = makeQuestionKey(gameId, questionIndex);
  if (finalizedQuestionKeys.has(key)) {
    return;
  }

  finalizedQuestionKeys.add(key);

  clearGameTimers(game);

  const questionStart = game.questionStartTime ?? Date.now();
  const questionDurationMs = question.timeLimitSec * 1000;

  const playerResults = game.players.map((player) => {
    const answer = game.playerAnswers.get(player.index);
    const answered = Boolean(answer);
    const correct = answered && answer?.answerIndex === question.correctIndex;

    let pointsEarned = 0;
    if (answer && correct) {
      const elapsedMs = Math.max(0, answer.timestamp - questionStart);
      const remainingMs = Math.max(0, questionDurationMs - elapsedMs);
      const rawScore = BASE_POINTS * (remainingMs / questionDurationMs);
      pointsEarned = clamp(Math.round(rawScore), 0, BASE_POINTS);
    }

    player.score += pointsEarned;

    return {
      name: player.name,
      answered,
      correct,
      pointsEarned,
      totalScore: player.score,
    };
  });

  broadcast(game, 'question_result', {
    questionIndex,
    correctIndex: question.correctIndex,
    playerResults,
  });

  const nextQuestionIndex = questionIndex + 1;
  if (nextQuestionIndex < game.questions.length) {
    game.transitionTimer = setTimeout(() => {
      const activeGame = gamesById.get(game.id);
      if (!activeGame || activeGame.status !== 'in_progress') {
        return;
      }

      sendQuestion(activeGame, nextQuestionIndex);
    }, NEXT_QUESTION_DELAY_MS);
    return;
  }

  finishGame(game);
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
    gameIdByUserId.set(user.index, game.id);
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

const handleStartGame = (ws: WebSocket, data: unknown): void => {
  const user = getUserBySocket(ws);
  if (!user) {
    sendError(ws, 'Please register/login first.');
    return;
  }

  if (!isRecord(data)) {
    sendError(ws, 'Invalid start_game payload.');
    return;
  }

  const payload = data as Partial<StartGameData>;
  const gameId = isNonEmptyString(payload.gameId) ? payload.gameId.trim() : '';
  if (!gameId) {
    sendError(ws, 'gameId is required.');
    return;
  }

  const game = gamesById.get(gameId);
  if (!game) {
    sendError(ws, 'Game not found.');
    return;
  }

  if (game.hostId !== user.index) {
    sendError(ws, 'Only host can start the game.');
    return;
  }

  if (game.status !== 'waiting') {
    sendError(ws, 'Game is not in waiting status.');
    return;
  }

  if (game.questions.length === 0) {
    sendError(ws, 'Game has no questions.');
    return;
  }

  if (game.players.length === 0) {
    sendError(ws, 'At least one player must join before starting.');
    return;
  }

  game.status = 'in_progress';
  sendQuestion(game, 0);
};

const handleAnswer = (ws: WebSocket, data: unknown): void => {
  const user = getUserBySocket(ws);
  if (!user) {
    sendError(ws, 'Please register/login first.');
    return;
  }

  if (!isRecord(data)) {
    sendError(ws, 'Invalid answer payload.');
    return;
  }

  const payload = data as Partial<AnswerData>;
  const gameId = isNonEmptyString(payload.gameId) ? payload.gameId.trim() : '';
  if (!gameId) {
    sendError(ws, 'gameId is required.');
    return;
  }

  if (!Number.isInteger(payload.questionIndex) || payload.questionIndex < 0) {
    sendError(ws, 'Invalid questionIndex.');
    return;
  }

  if (!Number.isInteger(payload.answerIndex) || payload.answerIndex < 0 || payload.answerIndex > 3) {
    sendError(ws, 'Invalid answerIndex.');
    return;
  }

  const game = gamesById.get(gameId);
  if (!game) {
    sendError(ws, 'Game not found.');
    return;
  }

  if (game.status !== 'in_progress') {
    sendError(ws, 'Game is not in progress.');
    return;
  }

  if (game.currentQuestion !== payload.questionIndex) {
    sendError(ws, 'Answer is for a non-current question.');
    return;
  }

  const player = game.players.find((item) => item.index === user.index);
  if (!player) {
    sendError(ws, 'Only joined players can submit answers.');
    return;
  }

  const questionKey = makeQuestionKey(game.id, payload.questionIndex);
  if (finalizedQuestionKeys.has(questionKey)) {
    sendError(ws, 'Question is already finalized.');
    return;
  }

  if (game.playerAnswers.has(user.index)) {
    sendError(ws, 'Answer already submitted for this question.');
    return;
  }

  game.playerAnswers.set(user.index, {
    answerIndex: payload.answerIndex,
    timestamp: Date.now(),
  });

  send(ws, 'answer_accepted', {
    questionIndex: payload.questionIndex,
  });

  if (allPlayersAnswered(game)) {
    finalizeQuestion(game.id, game.currentQuestion);
  }
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

  const gameId = gameIdByUserId.get(userId);
  if (!gameId) {
    return;
  }

  const game = gamesById.get(gameId);
  if (!game) {
    gameIdByUserId.delete(userId);
    return;
  }

  if (game.hostId === userId) {
    if (game.status === 'waiting') {
      broadcast(game, 'error', {
        message: 'Host disconnected. Game has been closed.',
      });
      cleanupGame(game.id);
      return;
    }

    if (game.status === 'in_progress') {
      finishGame(game);
      return;
    }

    cleanupGame(game.id);
    return;
  }

  const playerIndex = game.players.findIndex((player) => player.index === userId);
  if (playerIndex === -1) {
    if (gameIdByUserId.get(userId) === game.id) {
      gameIdByUserId.delete(userId);
    }
    return;
  }

  game.players.splice(playerIndex, 1);
  game.playerAnswers.delete(userId);

  if (gameIdByUserId.get(userId) === game.id) {
    gameIdByUserId.delete(userId);
  }

  broadcastPlayers(game);

  if (game.status === 'in_progress' && allPlayersAnswered(game)) {
    finalizeQuestion(game.id, game.currentQuestion);
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
        handleStartGame(ws, message.data);
        break;
      case 'answer':
        handleAnswer(ws, message.data);
        break;
      default:
        sendError(ws, `Unknown message type: ${message.type}`);
    }
  });

  ws.on('close', () => {
    unlinkSocket(ws);
  });

  ws.on('error', () => {
    unlinkSocket(ws);
  });
});

console.log(`WebSocket server is running on ws://localhost:${PORT}`);

export { send, broadcast, broadcastPlayers, parseIncomingMessage };
