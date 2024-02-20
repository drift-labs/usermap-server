import {
	Commitment,
	Connection,
	Keypair,
	MemcmpFilter,
	Context,
	KeyedAccountInfo,
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
import { RedisClient } from './utils/redisClient';
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

require('dotenv').config();

const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

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

	// For reconnection
	isUnsubscribing = false;
	resubTimeoutMs?: number | undefined;
	receivingData = false;
	timeoutId: NodeJS.Timeout | undefined;

	// For Health Metrics
	lastReceivedSlot: number;
	lastWriteTs: number;

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
	}

	async handleRpcResponse(
		context: Context,
		keyedAccountInfo: KeyedAccountInfo
	) {
		const incomingSlot = context.slot;

		this.lastReceivedSlot = incomingSlot;

		const existingData = await this.redisClient.client.get(
			keyedAccountInfo.accountId.toString()
		);
		if (!existingData) {
			this.lastWriteTs = Date.now();
			await this.redisClient.client.set(
				keyedAccountInfo.accountId.toString(),
				`${incomingSlot}::${keyedAccountInfo.accountInfo.data.toString('base64')}`
			);
			await this.redisClient.client.rpush(
				'user_pubkeys',
				keyedAccountInfo.accountId.toString()
			);
			return;
		}
		const existingSlot = existingData.split('::')[0];
		if (incomingSlot >= parseInt(existingSlot)) {
			this.lastWriteTs = Date.now();
			await this.redisClient.client.set(
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

	async subscribe(): Promise<void> {
		if (this.listenerId != null || this.isUnsubscribing) {
			return;
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
				console.log(`No ws data in ${this.resubTimeoutMs}ms, resubscribing`);
				await this.unsubscribe(true);
				this.receivingData = false;
				await this.subscribe();
			}
		}, this.resubTimeoutMs);
	}

	async unsubscribe(onResub = false): Promise<void> {
		if (!onResub) {
			this.resubTimeoutMs = undefined;
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
	const connection = new Connection(endpoint, 'recent');
	const wallet = new Wallet(new Keypair());
	const driftClient = new DriftClient({
		connection,
		wallet,
		env: driftEnv,
	});
	const program = driftClient.program;

	const redisClient = new RedisClient(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
	const filters = [getUserFilter(), getNonIdleUserFilter()];
	const subscriber = new WebsocketCacheProgramAccountSubscriber(
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
