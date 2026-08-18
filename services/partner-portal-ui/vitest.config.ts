import { defineConfig } from "vitest/config";
import vue from "@vitejs/plugin-vue";

export default defineConfig({
  plugins: [vue()],
  test: {
    environment: "jsdom",
    pool: "threads",
    // Первый динамический import() лениво-загруженного view в этом
    // sandbox-окружении реально занимает >10s (vite transform + worker
    // pool spawn под ограничениями песочницы) — дефолтный hookTimeout
    // (10s) слишком короткий именно здесь, не признак реальной проблемы в
    // самом коде (router/index.test.ts: последующие тесты в том же файле,
    // переиспользующие уже транспилированный модуль, укладываются в
    // миллисекунды).
    hookTimeout: 30000,
    testTimeout: 30000,
  },
});
