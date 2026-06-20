FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/collector/package.json apps/collector/package.json
RUN npm ci --workspace @brokara/collector

COPY apps/collector apps/collector
RUN npm run build --workspace @brokara/collector

RUN chown -R node:node /app
USER node
CMD ["npm", "run", "start", "--workspace", "@brokara/collector"]
