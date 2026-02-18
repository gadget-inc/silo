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
    // Use threads pool instead of forks - grpc-js doesn't work well with fork termination
    pool: "threads",
    // Timeout for tearing down test environment
    teardownTimeout: 1000,
    benchmark: {
      include: ["test/**/*.bench.ts"],
    },
  },
});
