"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WorkerMetrics = void 0;
exports.getWorkerMeter = getWorkerMeter;
const api_1 = require("@opentelemetry/api");
/** Metrics collected by the SiloWorker. */
class WorkerMetrics {
    /** Counter incremented on each poll call to leaseTasks. */
    pollCounter;
    /** Counter incremented when a poll returns zero tasks. */
    emptyPollCounter;
    /** Gauge reporting the number of available task slots (maxConcurrentTasks - active - queued). */
    availableTaskSlots;
    /** Default attributes applied to all metric recordings. */
    defaultAttributes;
    constructor(meter, taskGroup, availableSlotsFn) {
        this.defaultAttributes = { task_group: taskGroup };
        this.pollCounter = meter.createCounter("silo.worker.polls", {
            description: "Total number of poll calls to leaseTasks",
        });
        this.emptyPollCounter = meter.createCounter("silo.worker.polls.empty", {
            description: "Number of polls that returned zero tasks",
        });
        this.availableTaskSlots = meter.createObservableGauge("silo.worker.available_task_slots", {
            description: "Number of available task slots",
        });
        this.availableTaskSlots.addCallback((result) => {
            result.observe(availableSlotsFn(), this.defaultAttributes);
        });
    }
}
exports.WorkerMetrics = WorkerMetrics;
/** Default meter name used when no custom Meter is provided. */
const DEFAULT_METER_NAME = "silo-worker";
/** Get a Meter, using the provided one or falling back to the global MeterProvider. */
function getWorkerMeter(meter) {
    return meter ?? api_1.metrics.getMeter(DEFAULT_METER_NAME);
}
//# sourceMappingURL=metrics.js.map