"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const execa_1 = __importDefault(require("execa"));
const path_1 = __importDefault(require("path"));
const vitest_1 = require("vitest");
(0, vitest_1.describe)("package.json types exports", () => {
    (0, vitest_1.it)("should have the correct types exports", async () => {
        await (0, execa_1.default)("pnpm", ["exec", "attw", "--pack", "."], {
            cwd: path_1.default.resolve(__dirname, ".."),
        });
    }, 10000);
});
//# sourceMappingURL=are-the-types-wrong.test.js.map