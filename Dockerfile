# Official Node image from ECR Public — the bitnami catalog images were
# removed upstream (versioned tags 404 since late 2025). Ships git already.
FROM public.ecr.aws/docker/library/node:20.18.1
ENV NODE_ENV=production
RUN npm install -g typescript

WORKDIR /app
COPY drift-common /app/drift-common
COPY . .
WORKDIR /app/drift-common/protocol/sdk
RUN yarn
RUN yarn build
WORKDIR /app/drift-common/common-ts
RUN yarn
RUN yarn build
WORKDIR /app
RUN yarn
RUN yarn build

EXPOSE 9464

CMD [ "yarn", "publisher" ]