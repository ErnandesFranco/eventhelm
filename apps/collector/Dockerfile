FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/collector/package.json apps/collector/package.json
RUN npm ci --workspace @okcp/collector

COPY apps/collector apps/collector
RUN npm run build --workspace @okcp/collector

CMD ["npm", "run", "start", "--workspace", "@okcp/collector"]
