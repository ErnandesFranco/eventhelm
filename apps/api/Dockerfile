FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/api/package.json apps/api/package.json
RUN npm ci --workspace @eventhelm/api

COPY apps/api apps/api
RUN npm run build --workspace @eventhelm/api

EXPOSE 8080
RUN chown -R node:node /app
USER node
CMD ["npm", "run", "start", "--workspace", "@eventhelm/api"]
