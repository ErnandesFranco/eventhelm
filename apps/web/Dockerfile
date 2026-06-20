FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/web/package.json apps/web/package.json
RUN npm ci --workspace @brokara/web

COPY apps/web apps/web

EXPOSE 5173
RUN chown -R node:node /app
USER node
CMD ["npm", "run", "dev", "--workspace", "@brokara/web", "--", "--host", "0.0.0.0"]
