<p align="center">
  <img src="../../img/datalakes_banner.png" width="1000"/>
  <br>
  <em>Generate · Partition · Publish — your synthetic lake delivery kit.</em>
</p>

<p align="center">
  <a href="https://github.com/G-Schumacher44/ecom-datalake-exten/actions/workflows/ci.yml">
    <img alt="CI" src="https://github.com/G-Schumacher44/ecom-datalake-exten/actions/workflows/ci.yml/badge.svg?branch=main&event=push">
  </a>
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-alpha-lightgrey">
  <a href="https://github.com/G-Schumacher44/ecom-datalake-exten/releases/latest">
    <img alt="Release" src="https://img.shields.io/github/v/release/G-Schumacher44/ecom-datalake-exten?display_name=tag">
  </a>
</p>

---

# ⚙️ Data Lake Configuration Guide

Your quick reference for tuning the YAML configs that drive `ecomlake`. Use this alongside
[`gen_config/ecom_sales_gen_quick.yaml`](../../gen_config/ecom_sales_gen_quick.yaml) or the full template to dial volume, realism, and behavior without touching code.

> ⬅️ [Back to Project README](../../README.md)

---

## 📚 Table of Contents

- [⚙️ Data Lake Configuration Guide](#️-data-lake-configuration-guide)
  - [📚 Table of Contents](#-table-of-contents)
  - [📁 Lookup Foundations](#-lookup-foundations)
  - [🕒 Backlog & Temporal Controls](#-backlog--temporal-controls)
  - [🛒 Conversion & Cart Behavior](#-conversion--cart-behavior)
  - [📈 Seasonality & Segmentation](#-seasonality--segmentation)
  - [↩️ Returns & Financial Logic](#️-returns--financial-logic)
  - [💎 Loyalty & CLV Progression](#-loyalty--clv-progression)
  - [🧹 Messiness & QA](#-messiness--qa)
  - [🚀 Scaling Tips](#-scaling-tips)
  - [🗓️ Multi-date Exports & Hooks](#️-multi-date-exports--hooks)

---

### 🔌 Command Cheat Sheet

| Command | When to Use It | More Details |
| ------- | -------------- | ------------- |
| `ecomlake run-generator` | Produce fresh CSV artifacts from the generator after you tweak YAML knobs. | [CLI reference](CLI_REFERENCE.md#ecomlake-run-generator) |
| `ecomlake export-raw` | Lift a generator run into Parquet partitions + manifests before validation. | [CLI reference](CLI_REFERENCE.md#ecomlake-export-raw) |
| `ecomlake upload-raw` | Push locally validated partitions into your raw/bronze bucket. | [CLI reference](CLI_REFERENCE.md#ecomlake-upload-raw) |

Keep this trio in mind while adjusting configs—the rest of this guide describes how your YAML inputs influence each command’s behavior.

---

## 📁 Lookup Foundations

These controls live in the `lookup_config` block and shape the reusable dimensions that transactional tables reference.

- `product_catalog.num_products` → Distinct SKU count. Bump for wider catalog variety.
- `customers.num_customers` → Total customers (guests included). Primary driver for downstream cart/order scale.
- Additional fields (price ranges, age bands, margin spreads) keep your lookups realistic.

---

## 🕒 Backlog & Temporal Controls

Tune the time horizon for your synthetic history.

- `parameters.order_days_back` → Days of order history ending today.
- `parameters.signup_years` → How far back customer signups are seeded.
- `parameters.retention_shocks` → Month-specific multipliers (e.g., `'2024-05': 1.8` for a surge).

---

## 🛒 Conversion & Cart Behavior

Dial in the sales funnel and basket mix.

- `parameters.conversion_rate` → Baseline cart→order probability.
- `first_purchase_conversion_boost` → Signup-channel multipliers for a customer’s first conversion opportunity.
- `cart_behavior_by_tier` → `item_count_range` & `quantity_range` per loyalty tier.
- `abandoned_cart_emptied_prob` → Chance an unconverted cart is cleared to zero vs. left full.

---

## 📈 Seasonality & Segmentation

Model peak periods and channel bias.

- `seasonal_factors` → Monthly volume multipliers (e.g., `{11: 1.6}` for Black Friday).
- `signup_channel_distribution` / `order_channel_distribution` → Acquisition vs. purchase mix.
- `category_preference_by_signup_channel` → Skew categories by signup origin.

---

## ↩️ Returns & Financial Logic

Give operations and finance their storylines.

- `return_rate` and `return_rate_by_signup_channel` → Global vs. channel-specific return rates.
- `refund_behavior_by_reason` → Probability of full vs. partial refunds per reason code.
- `return_timing_distribution` → Cumulative distribution for how long after purchase returns arrive.
- `financials.shipping_business_costs` / `payment_fee_rates` → Business-side expense modeling.

---

## 💎 Loyalty & CLV Progression

Show how customers graduate through tiers and lifetime value.

- `tier_spend_thresholds` → Spend required to move Bronze→Silver→Gold→Platinum.
- `clv_spend_thresholds` → Buckets (Low / Medium / High) driven by cumulative spend.
- `repeat_purchase_settings` → Visit frequency and delay by channel+tier combos.

---

## 🧹 Messiness & QA

Keep datasets realistic (or pristine) depending on the exercise.

- CLI flag `--messiness-level` (`baseline`, `none`, `light_mess`, `medium_mess`, `heavy_mess`) toggles generation-time injection.
- Run `pytest` after major config changes to ensure QA rules remain intact.

---

## 🚀 Scaling Tips

- Start with the quick config for smoke tests; graduate to the full template once the pipeline is proven.
- Increase `customers.num_customers`, carts per day (`tables.shopping_carts.generate`), and `conversion_rate` together to hit target orders/day.
- Use `ecomlake export-raw --target-size-mb` to control Parquet chunk size (5–20 MB ideal for dev).
- Use `./scripts/smoke_test.sh` to run a short-range Backlog Bear check before long runs.

---

## 🗓️ Multi-date Exports & Hooks

- Combine partitions in a single run with `--dates`, `--start-date/--end-date`, or `--days`.
- Add `--post-export-hook module:function` to trigger custom QA, metrics, or alerts after each partition is written.

---

Need a deeper dive? Pop open the YAML next to this guide—the inline comments highlight how each block relates to these sections.

<p align="center">
  <sub>✨ Synthetic Data · Python · QA Framework ✨</sub>
</p>

<p align="center">
  <a href="../../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="CONFIG_GUIDE.md">⚙️ <b>Lake Config</b></a>
  &nbsp;·&nbsp;
  <a href="CLI_REFERENCE.md">🧭 <b>CLI Reference</b></a>
  &nbsp;·&nbsp;
  <a href="TESTING_GUIDE.md">🧪 <b>Testing</b></a>
  &nbsp;·&nbsp;
  <a href="workflows/BACKLOG_BEAR.md">🧸 <b>Workflows</b></a>
  &nbsp;·&nbsp;
  <a href="../ecom_generator/CONFIG_GUIDE.md">🛠️ <b>Generator Config</b></a>
</p>
