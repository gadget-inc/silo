/**
 * Combine multiple abort signals into one that aborts when ANY input signal aborts.
 * @internal
 */

export function combineAbortSignals(...signals: AbortSignal[]): AbortSignal {
  const controller = new AbortController();

  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort();
      return controller.signal;
    }

    signal.addEventListener(
      "abort",
      () => {
        controller.abort();
      },
      { once: true }
    );
  }

  return controller.signal;
}
