import {
	DriftClient,
	DriftEnv,
	PublicKey,
	Wallet,
	getNonIdleUserFilter,
	getUserFilter,
} from '@drift-labs/sdk';
import { Connection, Keypair, RpcResponseAndContext } from '@solana/web3.js';
import { sleep } from './utils/utils';
import { logger } from './utils/logger';
import { RedisClient } from './utils/redisClient';
import bs58 from 'bs58';

require('dotenv').config();

const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;

const REDIS_HOST = process.env.ELASTICACHE_HOST || 'localhost';
const REDIS_PORT = process.env.ELASTICACHE_PORT || '6379';

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

	if (idleUsers.size === 0) {
		throw new Error('UserMap size cant be 0');
	}

	console.log(`Number of idle users: ${idleUsers.size}`);

	const redisClient = new RedisClient(REDIS_HOST, REDIS_PORT);
	await redisClient.connect();

	const userList = await redisClient.client.lrange('user_pubkeys', 0, -1);
	const idleUserInCache = userList.filter((item) => idleUsers.has(item));

	for (const key of idleUserInCache) {
		console.log(`Pruning user: ${key}`);
		await redisClient.client.del(key);
		await redisClient.client.lrem('user_pubkeys', 0, key);
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
