# Drift Usermap
This project implements a caching system for Drift Protocol user accounts using Redis. It provides an API server that serves cached user account data and maintains real-time updates through WebSocket connections.

## Features

- WebSocket subscription to Drift program account changes
- Redis caching of user account data
- Support for different environments (devnet, mainnet)

## Prerequisites

- Node.js
- Redis server

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/drift-labs/usermap-server.git
   cd usermap-server
   ```
2. Install dependancies:
   ```bash
    yarn install
   ```
3. Set env variables:
   ```bash
    cp env.example .env
   ```
4. Setup a local instance of redis and update env variables to match
5. Run the publisher:
   ```bash
    yarn publisher
   ```