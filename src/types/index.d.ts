import { Express } from 'express';
import { Connection, Wallet } from '@solana/web3.js';
import { DriftClient } from '@drift-labs/sdk';
import { RedisClient } from './utils/redisClient';
import { WebsocketCacheProgramAccountSubscriber } from '../publisher';
import { grpcCacheProgramAccountSubscriber } from 'src/grpcPublisher';

export declare global {
	type Core = {
		app: Express;
		connection: Connection;
		wallet: Wallet;
		driftClient: DriftClient;
		redisClient: RedisClient;
		publisher:
			| WebsocketCacheProgramAccountSubscriber
			| grpcCacheProgramAccountSubscriber;
	};
}

// to make the file a module and avoid the TypeScript error
export {};
