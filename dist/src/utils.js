"use strict";
/**
 * Combine multiple abort signals into one that aborts when ANY input signal aborts.
 * @internal
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.combineAbortSignals = combineAbortSignals;
function combineAbortSignals(...signals) {
    const controller = new AbortController();
    for (const signal of signals) {
        if (signal.aborted) {
            controller.abort();
            return controller.signal;
        }
        signal.addEventListener("abort", () => {
            controller.abort();
        }, { once: true });
    }
    return controller.signal;
}
//# sourceMappingURL=utils.js.map