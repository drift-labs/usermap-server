import {
	DriftClient,
	DriftEnv,
	PublicKey,
	Wallet,
	getNonIdleUserFilter,
	getUserFilter,
} from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift/common';
import { Connection, Keypair, RpcResponseAndContext } from '@solana/web3.js';
import { sleep } from './utils/utils';
import { logger } from './utils/logger';
import bs58 from 'bs58';

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

	const program = driftClient.program;

	const {
		memcmp: { offset },
	} = getNonIdleUserFilter();

	const idleUserFilter = {
		memcmp: {
			offset,
			bytes: bs58.encode(Uint8Array.from([1])),
		},
	};

	const filters = [getUserFilter(), idleUserFilter];

	const rpcRequestArgs = [
		program.programId,
		{
			commitment: 'confirmed',
			filters,
			encoding: 'base64+zstd',
			withContext: true,
		},
	];

	// @ts-ignore
	const response = await program.provider.connection._rpcRequest(
		'getProgramAccounts',
		rpcRequestArgs
	);

	const rpcResponseAndContext: RpcResponseAndContext<
		Array<{ pubkey: PublicKey; account: { data: [string, string] } }>
	> = response.result;

	const idleUsers = new Set<string>();

	rpcResponseAndContext.value.map(async (programAccount) => {
		idleUsers.add(programAccount.pubkey.toString());
	});

	logger.info('Pruning idle users...');
	if (idleUsers.size === 0) {
		throw new Error('UserMap size cant be 0');
	}

	console.log(`Number of idle users: ${idleUsers.size}`);

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

	const userList = await redisClient.lRange('user_pubkeys', 0, -1);
	const idleUserInCache = userList.filter((item) => idleUsers.has(item));

	for (const key of idleUserInCache) {
		console.log(`Pruning user: ${key}`);
		await redisClient.delete(key);
		await redisClient.lRem('user_pubkeys', 0, key);
	}

	redisClient.disconnect();

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
