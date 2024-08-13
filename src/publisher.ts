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
import { runtimeSpecsGauge } from './core/metrics';
import { handleResponseTime } from './core/middleware';
import {
	DriftClient,
	DriftEnv,
	SlotSubscriber,
	Wallet,
	getNonIdleUserFilter,
	getUserFilter,
} from '@drift-labs/sdk';
import { sleep } from './utils/utils';
import { setupEndpoints } from './endpoints';
import { ZSTDDecoder } from 'zstddec';
import { RedisClient, RedisClientPrefix } from '@drift/common';
import { setGlobalDispatcher, Agent } from 'undici';

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

const SYNC_ON_STARTUP = process.env.SYNC_ON_STARTUP;

const endpoint = process.env.ENDPOINT!;
if (!endpoint) {
	logger.error('ENDPOINT env var is required');
	process.exit(1);
}
const wsEndpoint = process.env.WS_ENDPOINT || endpoint;
logger.info(`RPC endpoint:       ${endpoint}`);
logger.info(`WS endpoint:        ${wsEndpoint}`);
logger.info(`DriftEnv:           ${driftEnv}`);

const logFormat =
	':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent" :req[x-forwarded-for]';
const logHttp = morgan(logFormat, {
	skip: (_req, res) => res.statusCode <= 500,
});

const MAX_USER_ACCOUNT_SIZE_BYTES = 4376;

const app = express();
app.use(cors({ origin: '*' }));
app.use(compression());
app.set('trust proxy', 1);
app.use(logHttp);
app.use(handleResponseTime);

// Metrics defined here
const bootTimeMs = Date.now();
const commitHash = process.env.COMMIT;
runtimeSpecsGauge.addCallback((obs) => {
	obs.observe(bootTimeMs, {
		commit: commitHash,
		driftEnv,
		rpcEndpoint: endpoint,
		wsEndpoint: wsEndpoint,
	});
});

const server = http.createServer(app);

const httpPort = parseInt(process.env.PORT || '5001');
server.listen(httpPort);

// Default keepalive is 5s, since the AWS ALB timeout is 60 seconds, clients
// sometimes get 502s.
// https://shuheikagawa.com/blog/2019/04/25/keep-alive-timeout/
// https://stackoverflow.com/a/68922692
server.keepAliveTimeout = 61 * 1000;
server.headersTimeout = 65 * 1000;

export class WebsocketCacheProgramAccountSubscriber {
	program: Program;
	redisClient: RedisClient;
	listenerId: number | undefined;
	options: { filters: MemcmpFilter[]; commitment?: Commitment };
	syncInterval: NodeJS.Timeout | undefined;
	syncLock: boolean;

	// For reconnection
	isUnsubscribing = false;
	resubTimeoutMs?: number | undefined;
	receivingData = false;
	timeoutId: NodeJS.Timeout | undefined;

	// For Health Metrics
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
			await this.redisClient.setRaw(
				keyedAccountInfo.accountId.toString(),
				`${incomingSlot}::${keyedAccountInfo.accountInfo.data.toString('base64')}`
			);
			await this.redisClient.rPush(
				'user_pubkeys',
				keyedAccountInfo.accountId.toString()
			);
			return;
		}
		const existingSlot = existingData.split('::')[0];
		if (
			incomingSlot >= parseInt(existingSlot) ||
			isNaN(parseInt(existingSlot))
		) {
			this.lastWriteTs = Date.now();
			await this.redisClient.setRaw(
				keyedAccountInfo.accountId.toString(),
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
		} catch (e) {
			const err = e as Error;
			console.error(
				`Error in WebsocketCacheProgramAccountSubscriber.sync(): ${err.message} ${err.stack ?? ''}`
			);
		} finally {
			this.syncLock = false;
			logger.info('Releasing sync lock');
			logger.info(`Sync took ${performance.now() - start}ms`);
		}
	}

	async subscribe(): Promise<void> {
		await this.decoder.init();

		if (this.listenerId != null || this.isUnsubscribing) {
			return;
		}

		const syncInterval = setInterval(
			async () => {
				logger.info('Syncing on interval');
				await this.sync();
			},
			parseInt(process.env.SYNC_INTERVAL) || 90_000
		);

		this.syncInterval = syncInterval;

		if (SYNC_ON_STARTUP === 'true') {
			await this.sync();
		}

		this.listenerId = this.program.provider.connection.onProgramAccountChange(
			this.program.programId,
			(keyedAccountInfo, context) => {
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

	private setTimeout(): void {
		this.timeoutId = setTimeout(async () => {
			if (this.isUnsubscribing) {
				// If we are in the process of unsubscribing, do not attempt to resubscribe
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

async function main() {
	// Set up drift client for the program
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
	const subscriber = new WebsocketCacheProgramAccountSubscriber(
		//@ts-ignore
		program,
		redisClient,
		{ filters, commitment: 'confirmed' },
		30_000
	);

	const slotSubscriber = new SlotSubscriber(connection, {
		resubTimeoutMs: 2_000,
	});
	await slotSubscriber.subscribe();

	const core: Core = {
		app,
		connection,
		wallet,
		driftClient,
		redisClient,
		slotSubscriber,
		publisher: subscriber,
	};

	await subscriber.subscribe();

	setupEndpoints(core);

	console.log(``);
	console.log(`Server is set up and running. Port: ${httpPort}`);
	console.log(``);
}

async function recursiveTryCatch(f: () => Promise<void>) {
	try {
		await f();
	} catch (e) {
		console.error(e);
		await sleep(15000);
		await recursiveTryCatch(f);
	}
}

recursiveTryCatch(() => main());
