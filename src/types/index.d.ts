import { Express } from 'express';
import { Connection, Wallet } from '@solana/web3.js';
import { DriftClient, SlotSubscriber } from '@drift-labs/sdk';
import { RedisClient } from './utils/redisClient';
import { WebsocketCacheProgramAccountSubscriber } from '../publisher';

export declare global {
	type Core = {
		app: Express;
		connection: Connection;
		wallet: Wallet;
		driftClient: DriftClient;
		redisClient: RedisClient;
		slotSubscriber: SlotSubscriber;
		publisher: WebsocketCacheProgramAccountSubscriber;
	};
}

// to make the file a module and avoid the TypeScript error
export {};
