import { type Meter, type Counter, type ObservableGauge, type Attributes } from "@opentelemetry/api";
/** Metrics collected by the SiloWorker. */
export declare class WorkerMetrics {
    /** Counter incremented on each poll call to leaseTasks. */
    readonly pollCounter: Counter<Attributes>;
    /** Counter incremented when a poll returns zero tasks. */
    readonly emptyPollCounter: Counter<Attributes>;
    /** Gauge reporting the number of available task slots (maxConcurrentTasks - active - queued). */
    readonly availableTaskSlots: ObservableGauge<Attributes>;
    /** Default attributes applied to all metric recordings. */
    readonly defaultAttributes: Attributes;
    constructor(meter: Meter, taskGroup: string, availableSlotsFn: () => number);
}
/** Get a Meter, using the provided one or falling back to the global MeterProvider. */
export declare function getWorkerMeter(meter?: Meter): Meter;
//# sourceMappingURL=metrics.d.ts.map