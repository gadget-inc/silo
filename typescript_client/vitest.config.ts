import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["test/**/*.test.ts"],
    // Run test files sequentially to avoid integration tests interfering with each other
    fileParallelism: false,
    // Run tests within a file sequentially too
    sequence: {
      concurrent: false,
    },
  },
});
