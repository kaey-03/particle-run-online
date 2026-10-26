# 粒子梦境 Particle Run · 双人联机版
# CloudBase 云托管运行镜像（构建在腾讯云内网，使用腾讯镜像源拉基础镜像）
FROM mirror.ccs.tencentyun.com/library/node:20-alpine

WORKDIR /app

# 先装依赖（利用构建缓存）
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --registry=https://registry.npmmirror.com

# 拷贝服务端与前端页面
COPY server ./server
COPY public ./public

# 云托管默认对外端口为 80，服务端读取 PORT 环境变量
ENV PORT=80
EXPOSE 80

CMD ["node", "server/index.js"]
