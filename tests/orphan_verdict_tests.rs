//! Decision-table tests for the orphan holder classifier.
//!
//! The classifier is a pure function of the holder record, whether an
//! unexpired lease exists for the holder's task, the owning job's status row,
//! the grace and stale thresholds, and the clock.

use silo::concurrency::{OrphanReason, OrphanVerdict, classify_orphan_holder};
use silo::job::{JobStatus, JobStatusKind};
use silo::task::HolderRecord;

const GRACE_MS: i64 = 60_000;
const STALE_MS: i64 = 86_400_000;
const NOW_MS: i64 = 1_700_000_000_000;

fn owned(age_ms: i64, attempt_number: u32) -> HolderRecord {
    HolderRecord {
        granted_at_ms: NOW_MS - age_ms,
        job_id: Some("job-1".to_string()),
        attempt_number: Some(attempt_number),
    }
}

fn unowned(age_ms: i64) -> HolderRecord {
    HolderRecord {
        granted_at_ms: NOW_MS - age_ms,
        job_id: None,
        attempt_number: None,
    }
}

fn status(kind: JobStatusKind, current_attempt: Option<u32>) -> JobStatus {
    JobStatus {
        kind,
        changed_at_ms: NOW_MS,
        next_attempt_starts_after_ms: None,
        current_attempt,
    }
}

fn orphan(reason: OrphanReason) -> OrphanVerdict {
    OrphanVerdict::Orphan { reason }
}

struct Case {
    name: &'static str,
    holder: HolderRecord,
    lease_present: bool,
    status: Option<JobStatus>,
    stale_ms: i64,
    want: OrphanVerdict,
}

#[test]
fn classifier_decision_table() {
    let cases = [
        Case {
            name: "younger than grace is never classified even with no job",
            holder: owned(GRACE_MS - 1, 1),
            lease_present: false,
            status: None,
            stale_ms: STALE_MS,
            want: OrphanVerdict::Live,
        },
        Case {
            name: "unexpired lease keeps an owned holder live past stale with no job",
            holder: owned(STALE_MS + 1, 1),
            lease_present: true,
            status: None,
            stale_ms: STALE_MS,
            want: OrphanVerdict::Live,
        },
        Case {
            name: "unexpired lease keeps an unowned holder live past stale",
            holder: unowned(STALE_MS + 1),
            lease_present: true,
            status: None,
            stale_ms: STALE_MS,
            want: OrphanVerdict::Live,
        },
        Case {
            name: "owned holder with no job status row",
            holder: owned(GRACE_MS, 1),
            lease_present: false,
            status: None,
            stale_ms: STALE_MS,
            want: orphan(OrphanReason::JobMissing),
        },
        Case {
            name: "owned holder whose job succeeded",
            holder: owned(GRACE_MS, 1),
            lease_present: false,
            status: Some(status(JobStatusKind::Succeeded, None)),
            stale_ms: STALE_MS,
            want: orphan(OrphanReason::JobTerminal),
        },
        Case {
            name: "owned holder whose job failed",
            holder: owned(GRACE_MS, 1),
            lease_present: false,
            status: Some(status(JobStatusKind::Failed, None)),
            stale_ms: STALE_MS,
            want: orphan(OrphanReason::JobTerminal),
        },
        Case {
            name: "owned holder whose job was cancelled",
            holder: owned(GRACE_MS, 1),
            lease_present: false,
            status: Some(status(JobStatusKind::Cancelled, None)),
            stale_ms: STALE_MS,
            want: orphan(OrphanReason::JobTerminal),
        },
        Case {
            name: "owned holder whose job is running with no lease for its task",
            holder: owned(GRACE_MS, 1),
            lease_present: false,
            status: Some(status(JobStatusKind::Running, None)),
            stale_ms: STALE_MS,
            want: orphan(OrphanReason::RunningWithoutLease),
        },
        Case {
            name: "owned holder whose job is scheduled for a later attempt",
            holder: owned(GRACE_MS, 1),
            lease_present: false,
            status: Some(status(JobStatusKind::Scheduled, Some(2))),
            stale_ms: STALE_MS,
            want: orphan(OrphanReason::AttemptSuperseded),
        },
        Case {
            name: "owned holder whose job is parked mid-chain on the same attempt",
            holder: owned(STALE_MS - 1, 3),
            lease_present: false,
            status: Some(status(JobStatusKind::Scheduled, Some(3))),
            stale_ms: STALE_MS,
            want: OrphanVerdict::Live,
        },
        Case {
            name: "unowned holder within the stale threshold",
            holder: unowned(STALE_MS - 1),
            lease_present: false,
            status: None,
            stale_ms: STALE_MS,
            want: OrphanVerdict::Live,
        },
        Case {
            name: "unowned holder at the stale threshold",
            holder: unowned(STALE_MS),
            lease_present: false,
            status: None,
            stale_ms: STALE_MS,
            want: orphan(OrphanReason::Stale),
        },
        Case {
            name: "parked mid-chain holder at the stale threshold",
            holder: owned(STALE_MS, 3),
            lease_present: false,
            status: Some(status(JobStatusKind::Scheduled, Some(3))),
            stale_ms: STALE_MS,
            want: orphan(OrphanReason::Stale),
        },
        Case {
            name: "stale threshold of zero disables the age rule for unowned holders",
            holder: unowned(10 * STALE_MS),
            lease_present: false,
            status: None,
            stale_ms: 0,
            want: OrphanVerdict::Live,
        },
        Case {
            name: "stale threshold of zero disables the age rule for parked chains",
            holder: owned(10 * STALE_MS, 3),
            lease_present: false,
            status: Some(status(JobStatusKind::Scheduled, Some(3))),
            stale_ms: 0,
            want: OrphanVerdict::Live,
        },
    ];

    for case in cases {
        let got = classify_orphan_holder(
            &case.holder,
            case.lease_present,
            case.status.as_ref(),
            GRACE_MS,
            case.stale_ms,
            NOW_MS,
        );
        assert_eq!(got, case.want, "case: {}", case.name);
    }
}
