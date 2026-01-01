/** @type {import('next').NextConfig} */
const nextConfig = {
  // 環境變數前綴（公開給客戶端的變數）
  env: {
    NEXT_PUBLIC_DEFAULT_MIN_MARKET_CAP: process.env.DEFAULT_MIN_MARKET_CAP || '1000000000',
  },
};

module.exports = nextConfig;
