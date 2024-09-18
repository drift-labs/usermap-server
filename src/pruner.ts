import { DriftClient, DriftEnv, UserMap, Wallet } from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift/common';
import { Connection, Keypair } from '@solana/web3.js';
import { sleep } from './utils/utils';
import { logger } from './utils/logger';

require('dotenv').config();

const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;
const USE_ELASTICACHE = process.env.ELASTICACHE === 'true' || false;

const endpoint = process.env.ENDPOINT!;
if (!endpoint) {
	logger.error('ENDPOINT env var is required');
	process.exit(1);
}
const wsEndpoint = process.env.WS_ENDPOINT || endpoint;
logger.info(`RPC endpoint:       ${endpoint}`);
logger.info(`WS endpoint:        ${wsEndpoint}`);
logger.info(`DriftEnv:           ${driftEnv}`);

async function main() {
	const connection = new Connection(endpoint, 'recent');
	const wallet = new Wallet(new Keypair());
	const driftClient = new DriftClient({
		connection,
		wallet,
		env: driftEnv,
	});
	await driftClient.subscribe();

	const userMap = new UserMap({
		driftClient,
		connection,
		includeIdle: false,
		fastDecode: true,
		subscriptionConfig: {
			type: 'polling',
			frequency: 0,
			commitment: 'finalized',
		},
	});
	await userMap.sync();

	if (userMap.size() === 0) {
		throw new Error('UserMap size cant be 0');
	}

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

	await redisClient.connect();

	// Fetch the userMap and prune the redis cache from idle users
	let cursor = '0';
	let numIdleUsers = 0;
	do {
		const reply = await redisClient
			.forceGetClient()
			.scan(cursor, 'MATCH', '*', 'COUNT', 100);
		cursor = reply[0];
		const keys = reply[1];

		// Process the keys
		for (const key of keys) {
			if (key == 'user_pubkeys') continue;
			if (userMap.get(key) === undefined) {
				console.log(`Pruning idle or deleted user: ${key}`);
				await redisClient.delete(key);
				await redisClient.lRem('user_pubkeys', 0, key);
				numIdleUsers++;
			}
		}
	} while (cursor !== '0');

	redisClient.disconnect();

	console.log(`Pruned ${numIdleUsers} users`);
	console.log('Done!!');
	process.exit(0);
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
