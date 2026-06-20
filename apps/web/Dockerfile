FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/web/package.json apps/web/package.json
RUN npm ci --workspace @okcp/web

COPY apps/web apps/web

EXPOSE 5173
CMD ["npm", "run", "dev", "--workspace", "@okcp/web", "--", "--host", "0.0.0.0"]
