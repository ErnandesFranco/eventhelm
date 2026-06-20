FROM node:20-alpine

WORKDIR /app

ARG EVENTHELM_VERSION=0.1.0
ARG EVENTHELM_BUILD_SHA
ARG EVENTHELM_BUILD_TIME
ENV EVENTHELM_VERSION=$EVENTHELM_VERSION
ENV EVENTHELM_BUILD_SHA=$EVENTHELM_BUILD_SHA
ENV EVENTHELM_BUILD_TIME=$EVENTHELM_BUILD_TIME

COPY package.json package-lock.json ./
COPY apps/api/package.json apps/api/package.json
RUN npm ci --workspace @eventhelm/api

COPY apps/api apps/api
RUN npm run build --workspace @eventhelm/api

EXPOSE 8080
RUN chown -R node:node /app
USER node
CMD ["npm", "run", "start", "--workspace", "@eventhelm/api"]
