//! Cross-reference logic: leader schedule × validator geo → slot map and country aggregates.

use std::collections::BTreeMap;

use crate::domain::{
    CountrySummary, EpochSnapshot, EpochSummary, SlotEntry, ValidatorInfo,
};

/// Build absolute-slot → SlotEntry map for the entire epoch covered by `snap`.
/// Validators referenced in the schedule but missing from validators.app are
/// emitted with `None` country/data-center fields and stake=0.
pub fn build_slot_map(snap: &EpochSnapshot) -> BTreeMap<u64, SlotEntry> {
    let by_identity: BTreeMap<&str, &ValidatorInfo> = snap
        .validators
        .iter()
        .map(|v| (v.identity.as_str(), v))
        .collect();

    let epoch_first = snap.epoch.epoch_first_slot();
    let mut out = BTreeMap::new();

    for (identity, slot_indices) in &snap.schedule {
        let info = by_identity.get(identity.as_str());
        for &idx in slot_indices {
            let abs = epoch_first + idx;
            out.insert(
                abs,
                SlotEntry {
                    absolute_slot: abs,
                    identity: identity.clone(),
                    validator_name: info.and_then(|v| v.name.clone()),
                    country_code: info.and_then(|v| v.country_code.clone()),
                    data_center_key: info.and_then(|v| v.data_center_key.clone()),
                    stake_lamports: info.map(|v| v.active_stake_lamports).unwrap_or(0),
                },
            );
        }
    }
    out
}

/// Aggregate slot map into per-country counts and percentages.
/// Validators with no `country_code` are bucketed under `"??"`.
pub fn summarize(snap: &EpochSnapshot, slot_map: &BTreeMap<u64, SlotEntry>) -> EpochSummary {
    let total_slots = slot_map.len() as u64;
    let total_stake_lamports: u128 = snap
        .validators
        .iter()
        .map(|v| v.active_stake_lamports as u128)
        .sum();

    // Count slots per country
    let mut slots_per_country: BTreeMap<String, u64> = BTreeMap::new();
    let mut unknown_slots = 0u64;
    for entry in slot_map.values() {
        match &entry.country_code {
            Some(cc) => *slots_per_country.entry(cc.clone()).or_insert(0) += 1,
            None => unknown_slots += 1,
        }
    }

    // Count stake + validators per country (independent of schedule — true network shape)
    let mut stake_per_country: BTreeMap<String, u128> = BTreeMap::new();
    let mut validators_per_country: BTreeMap<String, u64> = BTreeMap::new();
    for v in &snap.validators {
        let cc = v.country_code.clone().unwrap_or_else(|| "??".to_string());
        *stake_per_country.entry(cc.clone()).or_insert(0) +=
            v.active_stake_lamports as u128;
        *validators_per_country.entry(cc).or_insert(0) += 1;
    }

    // Merge — start from countries that appear in either set
    let mut all_countries: std::collections::BTreeSet<String> = slots_per_country
        .keys()
        .chain(stake_per_country.keys())
        .cloned()
        .collect();
    if unknown_slots > 0 {
        all_countries.insert("??".to_string());
    }

    let mut countries: Vec<CountrySummary> = all_countries
        .into_iter()
        .map(|cc| {
            let slot_count = if cc == "??" {
                unknown_slots
            } else {
                slots_per_country.get(&cc).copied().unwrap_or(0)
            };
            let stake = stake_per_country.get(&cc).copied().unwrap_or(0);
            let validator_count = validators_per_country.get(&cc).copied().unwrap_or(0);
            CountrySummary {
                country_code: cc,
                slot_count,
                slot_percentage: pct(slot_count as u128, total_slots as u128),
                stake_lamports: stake,
                stake_percentage: pct(stake, total_stake_lamports),
                validator_count,
            }
        })
        .collect();
    // Sort by slot share descending
    countries.sort_by(|a, b| {
        b.slot_percentage
            .partial_cmp(&a.slot_percentage)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mapped_slots = total_slots - unknown_slots;

    EpochSummary {
        epoch: snap.epoch.epoch,
        total_slots,
        mapped_slots,
        unknown_slots,
        countries,
        total_stake_lamports,
    }
}

fn pct(part: u128, total: u128) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64) / (total as f64) * 100.0
    }
}

/// For `at <slot>` / `slots <range>`: lookup helpers (kept here for cohesion with
/// build_slot_map's contract).
pub fn lookup<'a>(
    slot_map: &'a BTreeMap<u64, SlotEntry>,
    slot: u64,
) -> Option<&'a SlotEntry> {
    slot_map.get(&slot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{EpochInfo, LeaderSchedule, ValidatorInfo};
    use chrono::Utc;
    use pretty_assertions::assert_eq;

    fn validator(id: &str, country: Option<&str>, stake: u64) -> ValidatorInfo {
        ValidatorInfo {
            identity: id.to_string(),
            name: Some(format!("{}-name", id)),
            vote_account: None,
            active_stake_lamports: stake,
            country_code: country.map(str::to_string),
            data_center_key: None,
            asn: None,
            asn_organization: None,
            ip: None,
        }
    }

    fn snap(
        validators: Vec<ValidatorInfo>,
        schedule: LeaderSchedule,
        first_slot: u64,
        slots: u64,
    ) -> EpochSnapshot {
        EpochSnapshot {
            fetched_at: Utc::now(),
            epoch: EpochInfo {
                epoch: 1,
                absolute_slot: first_slot,
                slot_index: 0,
                slots_in_epoch: slots,
            },
            validators,
            schedule,
        }
    }

    #[test]
    fn slot_map_assigns_absolute_slots_and_attaches_geo() {
        let mut sched = LeaderSchedule::new();
        sched.insert("VAL_DE".to_string(), vec![0, 1, 2, 3]);
        sched.insert("VAL_US".to_string(), vec![4, 5, 6, 7]);
        let s = snap(
            vec![
                validator("VAL_DE", Some("DE"), 1_000_000),
                validator("VAL_US", Some("US"), 500_000),
            ],
            sched,
            1_000,
            8,
        );

        let slot_map = build_slot_map(&s);
        assert_eq!(slot_map.len(), 8);
        assert_eq!(slot_map[&1_000].country_code.as_deref(), Some("DE"));
        assert_eq!(slot_map[&1_004].country_code.as_deref(), Some("US"));
    }

    #[test]
    fn unknown_validator_in_schedule_is_kept_with_empty_geo() {
        let mut sched = LeaderSchedule::new();
        sched.insert("VAL_DE".to_string(), vec![0, 1]);
        sched.insert("UNKNOWN".to_string(), vec![2, 3]);
        let s = snap(
            vec![validator("VAL_DE", Some("DE"), 1_000_000)],
            sched,
            500,
            4,
        );
        let slot_map = build_slot_map(&s);
        assert_eq!(slot_map.len(), 4);
        assert_eq!(slot_map[&502].country_code, None);
        assert_eq!(slot_map[&502].stake_lamports, 0);
    }

    #[test]
    fn summary_aggregates_by_country_and_computes_percentages() {
        let mut sched = LeaderSchedule::new();
        sched.insert("VAL_DE".to_string(), vec![0, 1, 2, 3, 4, 5]); // 6 slots = 60%
        sched.insert("VAL_US".to_string(), vec![6, 7, 8, 9]); // 4 slots = 40%
        let s = snap(
            vec![
                validator("VAL_DE", Some("DE"), 1_000_000),
                validator("VAL_US", Some("US"), 500_000),
            ],
            sched,
            0,
            10,
        );
        let slot_map = build_slot_map(&s);
        let sum = summarize(&s, &slot_map);
        assert_eq!(sum.total_slots, 10);
        assert_eq!(sum.mapped_slots, 10);
        assert_eq!(sum.unknown_slots, 0);
        // Top country first
        assert_eq!(sum.countries[0].country_code, "DE");
        assert!((sum.countries[0].slot_percentage - 60.0).abs() < 1e-9);
        assert_eq!(sum.countries[1].country_code, "US");
        assert!((sum.countries[1].slot_percentage - 40.0).abs() < 1e-9);
        assert!((sum.countries[0].stake_percentage - 66.6666).abs() < 1e-3);
    }

    #[test]
    fn unknown_geo_lands_in_double_question_bucket() {
        let mut sched = LeaderSchedule::new();
        sched.insert("VAL_DE".to_string(), vec![0]);
        sched.insert("VAL_NOGEO".to_string(), vec![1]);
        let s = snap(
            vec![
                validator("VAL_DE", Some("DE"), 100),
                validator("VAL_NOGEO", None, 100),
            ],
            sched,
            0,
            2,
        );
        let slot_map = build_slot_map(&s);
        let sum = summarize(&s, &slot_map);
        assert_eq!(sum.unknown_slots, 1);
        let qq = sum
            .countries
            .iter()
            .find(|c| c.country_code == "??")
            .expect("?? bucket present");
        assert_eq!(qq.slot_count, 1);
        assert_eq!(qq.validator_count, 1);
    }

    #[test]
    fn empty_schedule_yields_zero_summary() {
        let s = snap(vec![], LeaderSchedule::new(), 0, 0);
        let slot_map = build_slot_map(&s);
        let sum = summarize(&s, &slot_map);
        assert_eq!(sum.total_slots, 0);
        assert_eq!(sum.mapped_slots, 0);
        assert_eq!(sum.unknown_slots, 0);
    }

    #[test]
    fn lookup_returns_entry_for_known_slot() {
        let mut sched = LeaderSchedule::new();
        sched.insert("VAL_DE".to_string(), vec![3]);
        let s = snap(vec![validator("VAL_DE", Some("DE"), 1)], sched, 100, 10);
        let slot_map = build_slot_map(&s);
        assert_eq!(
            lookup(&slot_map, 103).map(|e| e.identity.as_str()),
            Some("VAL_DE")
        );
        assert!(lookup(&slot_map, 999).is_none());
    }
}
