import {
	Commitment,
	Connection,
	Keypair,
	MemcmpFilter,
	Context,
	KeyedAccountInfo,
	RpcResponseAndContext,
	PublicKey,
} from '@solana/web3.js';
import { logger } from './utils/logger';
import { AnchorProvider, Program } from '@coral-xyz/anchor';
import cors from 'cors';
import express from 'express';
import morgan from 'morgan';
import compression from 'compression';

import * as http from 'http';
import {
	updateUserPubkeyListLength,
	messageCounter,
	errorCounter,
} from './core/metrics';
import {
	DriftClient,
	DriftEnv,
	Wallet,
	getNonIdleUserFilter,
	getUserFilter,
} from '@drift-labs/sdk';
import { sleep } from './utils/utils';
import { setupEndpoints } from './endpoints';
import { ZSTDDecoder } from 'zstddec';
import { COMMON_UI_UTILS } from '@drift/common';
import { RedisClient, RedisClientPrefix } from '@drift/common/clients';
import { setGlobalDispatcher, Agent } from 'undici';

import { ClientDuplexStream } from '@grpc/grpc-js';
import {
	CommitmentLevel,
	SubscribeRequest,
	SubscribeUpdate,
} from '@triton-one/yellowstone-grpc';
import Client from '@triton-one/yellowstone-grpc';

import bs58 from 'bs58';
import { Counter } from '@opentelemetry/api';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

require('dotenv').config();

const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;
const USE_ELASTICACHE = process.env.ELASTICACHE === 'true' || false;

const useGrpc = process.env.USE_GRPC === 'true';
const token = process.env.TOKEN;
const endpoint = useGrpc
	? token
		? process.env.ENDPOINT + `/${token}`
		: process.env.ENDPOINT
	: process.env.ENDPOINT;

if (!endpoint || (useGrpc && !token)) {
	logger.error('ENDPOINT (and TOKEN if using gRPC) env var required');
	process.exit(1);
}

const wsEndpoint = process.env.WS_ENDPOINT || endpoint;
logger.info(`RPC endpoint:       ${endpoint}`);
logger.info(`WS endpoint:        ${wsEndpoint}`);
logger.info(`DriftEnv:           ${driftEnv}`);
logger.info(`Using GRPC:           ${useGrpc}`);

const SYNC_ON_STARTUP = process.env.SYNC_ON_STARTUP;
const SYNC_INTERVAL = parseInt(process.env.SYNC_INTERVAL || '90000');

const EXPIRY_MULTIPLIER = 4;
const MAX_USER_ACCOUNT_SIZE_BYTES = 4376;

export class WebsocketCacheProgramAccountSubscriber {
	program: Program;
	redisClient: RedisClient;
	listenerId: number | undefined;
	options: { filters: MemcmpFilter[]; commitment?: Commitment };
	syncInterval: NodeJS.Timeout | undefined;
	syncLock: boolean;

	isUnsubscribing = false;
	resubTimeoutMs?: number | undefined;
	receivingData = false;
	timeoutId: NodeJS.Timeout | undefined;

	lastReceivedSlot: number;
	lastWriteTs: number;

	decoder: ZSTDDecoder;

	constructor(
		program: Program,
		redisClient: RedisClient,
		options: { filters: MemcmpFilter[]; commitment?: Commitment } = {
			filters: [],
		},
		resubTimeoutMs?: number | undefined
	) {
		this.program = program;
		this.redisClient = redisClient;
		this.options = options;
		this.resubTimeoutMs = resubTimeoutMs;
		this.receivingData = false;
		this.decoder = new ZSTDDecoder();
	}

	async handleRpcResponse(
		context: Context,
		keyedAccountInfo: KeyedAccountInfo
	) {
		const incomingSlot = context.slot;
		this.lastReceivedSlot = incomingSlot;

		const existingData = await this.redisClient.getRaw(
			keyedAccountInfo.accountId.toString()
		);

		if (!existingData) {
			this.lastWriteTs = Date.now();
			await this.redisClient
				.forceGetClient()
				.setex(
					keyedAccountInfo.accountId.toString(),
					(SYNC_INTERVAL * EXPIRY_MULTIPLIER) / 1000,
					`${incomingSlot}::${keyedAccountInfo.accountInfo.data.toString('base64')}`
				);
			return;
		}

		const existingSlot = existingData.split('::')[0];
		if (
			incomingSlot >= parseInt(existingSlot) ||
			isNaN(parseInt(existingSlot))
		) {
			this.lastWriteTs = Date.now();
			await this.redisClient
				.forceGetClient()
				.setex(
					keyedAccountInfo.accountId.toString(),
					(SYNC_INTERVAL * EXPIRY_MULTIPLIER) / 1000,
					`${incomingSlot}::${keyedAccountInfo.accountInfo.data.toString('base64')}`
				);
			return;
		}
	}

	public getHealthMetrics() {
		return {
			isSubscribed: this.listenerId != null,
			lastReceivedSlot: this.lastReceivedSlot,
			lastWriteTs: this.lastWriteTs,
		};
	}

	async sync(): Promise<void> {
		const start = performance.now();
		try {
			if (this.syncLock) {
				logger.info('SYNC LOCKED');
				return;
			}

			logger.info('Running Sync');
			this.syncLock = true;
			messageCounter?.add(1, { label: 'sync' });

			const filters = [getUserFilter(), getNonIdleUserFilter()];
			const rpcRequestArgs = [
				this.program.programId,
				{
					commitment: 'confirmed',
					filters,
					encoding: 'base64+zstd',
					withContext: true,
				},
			];

			// @ts-ignore
			const response = await this.program.provider.connection._rpcRequest(
				'getProgramAccounts',
				rpcRequestArgs
			);

			const rpcResponseAndContext: RpcResponseAndContext<
				Array<{ pubkey: PublicKey; account: { data: [string, string] } }>
			> = response.result;

			const context = rpcResponseAndContext.context;
			const programAccountBufferMap = new Map<string, Buffer>();

			const decodingPromises = rpcResponseAndContext.value.map(
				async (programAccount) => {
					const compressedUserData = Buffer.from(
						programAccount.account.data[0],
						'base64'
					);
					const userBuffer = this.decoder.decode(
						compressedUserData,
						MAX_USER_ACCOUNT_SIZE_BYTES
					);
					programAccountBufferMap.set(
						programAccount.pubkey.toString(),
						Buffer.from(userBuffer)
					);
				}
			);

			await Promise.all(decodingPromises);
			logger.info(`${programAccountBufferMap.size} users to sync`);

			const promises = Array.from(programAccountBufferMap.entries()).map(
				([key, buffer]) =>
					(async () => {
						const keyedAccountInfo: KeyedAccountInfo = {
							accountId: new PublicKey(key),
							accountInfo: {
								data: buffer,
								executable: false,
								owner: this.program.programId,
								lamports: 0,
							},
						};
						await this.handleRpcResponse(context, keyedAccountInfo);
					})()
			);

			await Promise.all(promises);
			await this.syncPubKeys(programAccountBufferMap);

			const updatedListLength = await this.redisClient.lLen('user_pubkeys');
			updateUserPubkeyListLength(updatedListLength);
		} catch (e) {
			const err = e as Error;
			errorCounter?.add(1, {
				label: 'WebsocketCacheProgramAccountSubscriber.sync',
			});
			console.error(
				`Error in WebsocketCacheProgramAccountSubscriber.sync(): ${err.message} ${err.stack ?? ''}`
			);
		} finally {
			this.syncLock = false;
			logger.info('Releasing sync lock');
			logger.info(`Sync took ${performance.now() - start}ms`);
		}
	}

	async syncPubKeys(
		programAccountBufferMap: Map<string, Buffer>
	): Promise<void> {
		const newKeys = Array.from(programAccountBufferMap.keys());
		const currentKeys = await this.redisClient.lRange('user_pubkeys', 0, -1);

		const keysToAdd = newKeys.filter((key) => !currentKeys.includes(key));
		const keysToRemove = currentKeys.filter((key) => !newKeys.includes(key));

		const removalBatches = COMMON_UI_UTILS.chunks(keysToRemove, 100);
		for (const batch of removalBatches) {
			await Promise.all(
				batch.map((key) => this.redisClient.lRem('user_pubkeys', 0, key))
			);
		}

		const additionBatches = COMMON_UI_UTILS.chunks(keysToAdd, 100);
		for (const batch of additionBatches) {
			await this.redisClient.rPush('user_pubkeys', ...batch);
		}

		logger.info(
			`Synchronized user_pubkeys: Added ${keysToAdd.length}, Removed ${keysToRemove.length}`
		);
	}

	async checkSync(): Promise<void> {
		if (this.syncLock) {
			logger.info('SYNC LOCKED DURING CHECK');
			return;
		}

		const storedUserPubkeys = await this.redisClient.lRange(
			'user_pubkeys',
			0,
			-1
		);

		const removedKeys = [];
		await Promise.all(
			storedUserPubkeys.map(async (pubkey) => {
				const exists = await this.redisClient.forceGetClient().exists(pubkey);
				if (!exists) {
					removedKeys.push(pubkey);
				}
			})
		);

		if (removedKeys.length > 0) {
			await Promise.all(
				removedKeys.map(async (pubkey) => {
					this.redisClient.lRem('user_pubkeys', 0, pubkey);
				})
			);
		}

		const updatedListLength = await this.redisClient.lLen('user_pubkeys');
		updateUserPubkeyListLength(updatedListLength);

		logger.warn(
			`Found ${removedKeys.length} keys to remove from user_pubkeys list`
		);
	}

	async subscribe(): Promise<void> {
		await this.decoder.init();

		if (this.listenerId != null || this.isUnsubscribing) {
			return;
		}

		let syncCount = 0;
		const syncInterval = setInterval(async () => {
			logger.info('Syncing on interval');
			await this.sync();

			if (syncCount % EXPIRY_MULTIPLIER === 0) {
				logger.info('Checking sync on interval');
				await this.checkSync();
			}

			syncCount++;
		}, SYNC_INTERVAL);

		this.syncInterval = syncInterval;

		if (SYNC_ON_STARTUP === 'true') {
			await this.sync();
		}

		this.listenerId = this.program.provider.connection.onProgramAccountChange(
			this.program.programId,
			(keyedAccountInfo, context) => {
				messageCounter?.add(1, { label: 'sync' });
				if (this.resubTimeoutMs) {
					clearTimeout(this.timeoutId);
					this.handleRpcResponse(context, keyedAccountInfo);
					this.setTimeout();
				} else {
					this.handleRpcResponse(context, keyedAccountInfo);
				}
			},
			this.options.commitment ??
				(this.program.provider as AnchorProvider).opts.commitment,
			this.options.filters
		);

		if (this.resubTimeoutMs) {
			this.receivingData = true;
			this.setTimeout();
		}
	}

	protected setTimeout(): void {
		this.timeoutId = setTimeout(async () => {
			if (this.isUnsubscribing) {
				return;
			}

			if (this.receivingData) {
				logger.info(`No ws data in ${this.resubTimeoutMs}ms, resubscribing`);
				await this.unsubscribe(true);
				this.receivingData = false;
				await this.subscribe();
			}
		}, this.resubTimeoutMs);
	}

	async unsubscribe(onResub = false): Promise<void> {
		if (!onResub) {
			this.resubTimeoutMs = undefined;
			if (this.syncInterval) {
				clearInterval(this.syncInterval);
			}
		}
		this.isUnsubscribing = true;
		clearTimeout(this.timeoutId);
		this.timeoutId = undefined;

		if (this.listenerId != null) {
			await this.program.provider.connection
				.removeAccountChangeListener(this.listenerId)
				.then(() => {
					this.listenerId = undefined;
					this.isUnsubscribing = false;
				});
			return;
		} else {
			this.isUnsubscribing = false;
		}
		return;
	}
}

class grpcCacheProgramAccountSubscriber extends WebsocketCacheProgramAccountSubscriber {
	client: Client;
	stream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>;

	constructor(
		endpoint: string,
		token: string,
		program: Program,
		redisClient: RedisClient,
		options: { filters: MemcmpFilter[]; commitment?: Commitment } = {
			filters: [],
		},
		resubTimeoutMs?: number | undefined
	) {
		super(program, redisClient, options, resubTimeoutMs);
		this.client = new Client(endpoint, token, {
			'grpc.keepalive_time_ms': 10_000,
			'grpc.keepalive_timeout_ms': 1_000,
			'grpc.keepalive_permit_without_calls': 1,
			'grpc.max_receive_message_length': 100 * 1024 * 1024,
		});
	}

	async subscribe(): Promise<void> {
		await this.decoder.init();

		if (this.listenerId != null || this.isUnsubscribing) {
			return;
		}

		let syncCount = 0;
		const syncInterval = setInterval(async () => {
			logger.info('Syncing on interval');
			await this.sync();

			if (syncCount % EXPIRY_MULTIPLIER === 0) {
				logger.info('Checking sync on interval');
				await this.checkSync();
			}

			syncCount++;
		}, SYNC_INTERVAL);

		this.syncInterval = syncInterval;

		if (SYNC_ON_STARTUP === 'true') {
			await this.sync();
		}

		this.stream = await this.client.subscribe();
		const filters = this.options.filters.map((filter) => ({
			memcmp: {
				offset: filter.memcmp.offset.toString(),
				bytes: bs58.decode(filter.memcmp.bytes),
			},
		}));
		const request: SubscribeRequest = {
			slots: {},
			accounts: {
				drift: {
					account: [],
					owner: [this.program.programId.toBase58()],
					filters,
				},
			},
			transactions: {},
			blocks: {},
			blocksMeta: {},
			accountsDataSlice: [],
			commitment: CommitmentLevel.CONFIRMED,
			entry: {},
			transactionsStatus: {},
		};

		this.stream.on('data', (chunk: SubscribeUpdate) => {
			if (!chunk.account) {
				return;
			}
			const slot = Number(chunk.account.slot);
			const accountInfo = {
				owner: new PublicKey(chunk.account.account.owner),
				lamports: Number(chunk.account.account.lamports),
				data: Buffer.from(chunk.account.account.data),
				executable: chunk.account.account.executable,
				rentEpoch: Number(chunk.account.account.rentEpoch),
			};

			messageCounter?.add(1, { label: 'grpc_data' });

			if (this.resubTimeoutMs) {
				this.receivingData = true;
				clearTimeout(this.timeoutId);
				this.handleRpcResponse(
					{ slot },
					{
						accountId: new PublicKey(chunk.account.account.pubkey),
						accountInfo,
					}
				);
				this.setTimeout();
			} else {
				this.handleRpcResponse(
					{ slot },
					{
						accountId: new PublicKey(chunk.account.account.pubkey),
						accountInfo,
					}
				);
			}
		});
		this.stream.on('error', (err) => {
			logger.error(`GRPC error: ${err.message}`);
			console.error('GRPC stream error:', err);
			errorCounter?.add(1, { label: 'grpc_stream_error', error: err.message });
		});

		return new Promise<void>((resolve, reject) => {
			this.stream.write(request, (err) => {
				if (err === null || err === undefined) {
					this.listenerId = 1;
					if (this.resubTimeoutMs) {
						this.receivingData = true;
						this.setTimeout();
					}
					resolve();
				} else {
					reject(err);
				}
			});
		}).catch((reason) => {
			errorCounter?.add(1, {
				label: 'grpcCacheProgramAccountSubscriber.subscribe',
			});
			console.error(
				'Error in grpcCacheProgramAccountSubscriber.subscribe():',
				reason
			);
			throw reason;
		});
	}

	public async unsubscribe(onResub = false): Promise<void> {
		if (!onResub && this.resubTimeoutMs) {
			this.resubTimeoutMs = undefined;
		}
		this.isUnsubscribing = true;
		clearTimeout(this.timeoutId);
		this.timeoutId = undefined;

		if (this.listenerId != null) {
			const promise = new Promise<void>((resolve, reject) => {
				const request: SubscribeRequest = {
					slots: {},
					accounts: {},
					transactions: {},
					blocks: {},
					blocksMeta: {},
					accountsDataSlice: [],
					entry: {},
					transactionsStatus: {},
				};
				this.stream.write(request, (err) => {
					if (err === null || err === undefined) {
						this.listenerId = undefined;
						this.isUnsubscribing = false;
						resolve();
					} else {
						reject(err);
					}
				});
			})
				.catch((reason) => {
					errorCounter?.add(1, {
						label: 'grpcCacheProgramAccountSubscriber.unsubscribe',
					});
					console.error(
						'Error in grpcCacheProgramAccountSubscriber.unsubscribe():',
						reason
					);
					throw reason;
				})
				.finally(() => {
					this.stream.end();
				});
			return promise;
		} else {
			this.isUnsubscribing = false;
		}
	}
}

export function setupServer(): { app: express.Express; httpPort: number } {
	const logFormat =
		':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent" :req[x-forwarded-for]';

	const logHttp = morgan(logFormat, {
		skip: (_req, res) => res.statusCode <= 500,
	});

	const app = express();
	app.use(cors({ origin: '*' }));
	app.use(compression());
	app.set('trust proxy', 1);
	app.use(logHttp);

	const server = http.createServer(app);

	const httpPort = parseInt(process.env.PORT || '5001');
	server.listen(httpPort);

	server.keepAliveTimeout = 61 * 1000;
	server.headersTimeout = 65 * 1000;

	return { app, httpPort };
}

async function main() {
	const connection = new Connection(endpoint, 'confirmed');
	const wallet = new Wallet(new Keypair());
	const driftClient = new DriftClient({
		connection,
		wallet,
		env: driftEnv,
	});
	const program = driftClient.program;

	const redisClient = USE_ELASTICACHE
		? new RedisClient({
				prefix: RedisClientPrefix.USER_MAP,
			})
		: new RedisClient({
				host: REDIS_HOST,
				port: REDIS_PORT,
				cluster: false,
				opts: { password: REDIS_PASSWORD, tls: null },
			});

	const filters = [getUserFilter(), getNonIdleUserFilter()];

	const subscriber = useGrpc
		? new grpcCacheProgramAccountSubscriber(
				endpoint!,
				token!,
				//@ts-ignore
				program,
				redisClient,
				{ filters, commitment: 'confirmed' },
				30_000
			)
		: new WebsocketCacheProgramAccountSubscriber(
				//@ts-ignore
				program,
				redisClient,
				{ filters, commitment: 'confirmed' },
				30_000
			);

	const { app, httpPort } = setupServer();

	const core: Core = {
		app,
		connection,
		wallet,
		driftClient,
		redisClient,
		publisher: subscriber,
	};

	await subscriber.subscribe();
	setupEndpoints(core);

	console.log(``);
	console.log(`Server is set up and running: ${httpPort}`);
	console.log(``);
}

async function recursiveTryCatch(f: () => Promise<void>) {
	try {
		await f();
	} catch (e) {
		errorCounter?.add(1, { label: 'recursiveTryCatch' });
		console.error('Error in recursiveTryCatch:', e);
		await sleep(15000);
		await recursiveTryCatch(f);
	}
}

recursiveTryCatch(() => main());
