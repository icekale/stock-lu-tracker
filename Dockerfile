FROM node:24-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=8787
ENV TESSDATA_PREFIX=/app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src
COPY public ./public
COPY eng.traineddata chi_sim.traineddata ./

RUN mkdir -p /app/data && chown -R node:node /app

USER node

EXPOSE 8787

CMD ["node", "src/server.js"]
