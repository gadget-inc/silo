import {
  type Meter,
  type Counter,
  type ObservableGauge,
  type Attributes,
  metrics,
} from "@opentelemetry/api";

/** Metrics collected by the SiloWorker. */
export class WorkerMetrics {
  /** Counter incremented on each poll call to leaseTasks. */
  readonly pollCounter: Counter<Attributes>;
  /** Counter incremented when a poll returns zero tasks. */
  readonly emptyPollCounter: Counter<Attributes>;
  /** Counter tracking the total number of tasks returned from polls. */
  readonly pollTasksReturnedCounter: Counter<Attributes>;
  /** Gauge reporting the number of available task slots (maxConcurrentTasks - active - queued). */
  readonly availableTaskSlots: ObservableGauge<Attributes>;
  /** Default attributes applied to all metric recordings. */
  readonly defaultAttributes: Attributes;

  constructor(meter: Meter, taskGroup: string, availableSlotsFn: () => number) {
    this.defaultAttributes = { task_group: taskGroup };

    this.pollCounter = meter.createCounter("silo.worker.polls", {
      description: "Total number of poll calls to leaseTasks",
    });

    this.emptyPollCounter = meter.createCounter("silo.worker.polls.empty", {
      description: "Number of polls that returned zero tasks",
    });

    this.pollTasksReturnedCounter = meter.createCounter("silo.worker.polls.tasks_returned", {
      description: "Total number of tasks returned from polls",
    });

    this.availableTaskSlots = meter.createObservableGauge("silo.worker.available_task_slots", {
      description: "Number of available task slots",
    });
    this.availableTaskSlots.addCallback((result) => {
      result.observe(availableSlotsFn(), this.defaultAttributes);
    });
  }
}

/** Default meter name used when no custom Meter is provided. */
const DEFAULT_METER_NAME = "silo-worker";

/** Get a Meter, using the provided one or falling back to the global MeterProvider. */
export function getWorkerMeter(meter?: Meter): Meter {
  return meter ?? metrics.getMeter(DEFAULT_METER_NAME);
}
