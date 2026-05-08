use comfy_table::{Cell, ContentArrangement, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};

use crate::domain::{EpochSummary, SlotEntry};

pub fn print_summary(s: &EpochSummary) {
    println!(
        "\nEpoch {}  •  {} / {} slots mapped ({:.3}%)\n",
        s.epoch,
        s.mapped_slots,
        s.total_slots,
        if s.total_slots == 0 {
            0.0
        } else {
            (s.mapped_slots as f64) / (s.total_slots as f64) * 100.0
        }
    );

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Country"),
            Cell::new("Slots %").set_alignment(comfy_table::CellAlignment::Right),
            Cell::new("Stake %").set_alignment(comfy_table::CellAlignment::Right),
            Cell::new("Slot count").set_alignment(comfy_table::CellAlignment::Right),
            Cell::new("Validators").set_alignment(comfy_table::CellAlignment::Right),
        ]);

    for c in &s.countries {
        table.add_row(vec![
            Cell::new(format_country(&c.country_code)),
            Cell::new(format!("{:>7.3}%", c.slot_percentage))
                .set_alignment(comfy_table::CellAlignment::Right),
            Cell::new(format!("{:>7.3}%", c.stake_percentage))
                .set_alignment(comfy_table::CellAlignment::Right),
            Cell::new(c.slot_count).set_alignment(comfy_table::CellAlignment::Right),
            Cell::new(c.validator_count).set_alignment(comfy_table::CellAlignment::Right),
        ]);
    }
    println!("{table}");

    println!();
    print_region_rollups(s);
    println!();
}

fn print_region_rollups(s: &EpochSummary) {
    let eu = sum_slots(s, &EU_CODES);
    let na = sum_slots(s, &NA_CODES);
    let apac = sum_slots(s, &APAC_CODES);
    let other = 100.0 - eu - na - apac;
    println!("Regional rollups (slot %):");
    println!("  🇪🇺 EU       : {:>6.2}%", eu);
    println!("  🌎 NA       : {:>6.2}%", na);
    println!("  🌏 APAC     : {:>6.2}%", apac);
    println!("  🌍 Other/?  : {:>6.2}%", other.max(0.0));
}

fn sum_slots(s: &EpochSummary, codes: &[&str]) -> f64 {
    s.countries
        .iter()
        .filter(|c| codes.iter().any(|k| k.eq_ignore_ascii_case(&c.country_code)))
        .map(|c| c.slot_percentage)
        .sum()
}

const EU_CODES: &[&str] = &[
    "DE", "NL", "FR", "GB", "UK", "IE", "PL", "CZ", "AT", "SE", "FI", "DK", "ES", "IT", "BE",
    "LU", "CH", "NO", "EE", "LT", "LV", "PT", "GR", "HU", "RO", "BG", "SK", "SI", "HR",
];
const NA_CODES: &[&str] = &["US", "CA", "MX"];
const APAC_CODES: &[&str] = &[
    "JP", "SG", "KR", "HK", "TW", "AU", "NZ", "IN", "TH", "MY", "ID", "PH", "VN",
];

pub fn print_slot_range(entries: &[SlotEntry]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Slot"),
            Cell::new("Validator"),
            Cell::new("Identity (short)"),
            Cell::new("Country"),
            Cell::new("DC"),
            Cell::new("Stake (SOL)").set_alignment(comfy_table::CellAlignment::Right),
        ]);

    for e in entries {
        table.add_row(vec![
            Cell::new(e.absolute_slot),
            Cell::new(e.validator_name.clone().unwrap_or_else(|| "-".into())),
            Cell::new(short_id(&e.identity)),
            Cell::new(format_country(&e.country_code.clone().unwrap_or_else(|| "??".into()))),
            Cell::new(e.data_center_key.clone().unwrap_or_else(|| "-".into())),
            Cell::new(format!("{:.0}", lamports_to_sol(e.stake_lamports)))
                .set_alignment(comfy_table::CellAlignment::Right),
        ]);
    }
    println!("{table}");
}

fn short_id(id: &str) -> String {
    if id.len() <= 12 {
        id.to_string()
    } else {
        format!("{}…{}", &id[..6], &id[id.len() - 4..])
    }
}

fn lamports_to_sol(l: u64) -> f64 {
    (l as f64) / 1_000_000_000.0
}

fn format_country(cc: &str) -> String {
    if cc == "??" || cc.is_empty() {
        return "??".into();
    }
    let flag = country_flag(cc);
    format!("{} {}", flag, cc)
}

fn country_flag(cc: &str) -> String {
    if cc.len() != 2 {
        return "🏳️".into();
    }
    let bytes = cc.to_uppercase();
    let mut chars = bytes.chars();
    let a = chars.next().unwrap();
    let b = chars.next().unwrap();
    if !a.is_ascii_alphabetic() || !b.is_ascii_alphabetic() {
        return "🏳️".into();
    }
    let a = (a as u32) - ('A' as u32) + 0x1F1E6;
    let b = (b as u32) - ('A' as u32) + 0x1F1E6;
    format!(
        "{}{}",
        char::from_u32(a).unwrap_or('?'),
        char::from_u32(b).unwrap_or('?')
    )
}
