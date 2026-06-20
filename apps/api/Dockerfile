FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/api/package.json apps/api/package.json
RUN npm ci --workspace @okcp/api

COPY apps/api apps/api
RUN npm run build --workspace @okcp/api

EXPOSE 8080
CMD ["npm", "run", "start", "--workspace", "@okcp/api"]
