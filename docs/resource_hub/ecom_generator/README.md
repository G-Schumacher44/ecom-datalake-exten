<p align="center">
  <img src="../img/sql_stories_logo.png" width="1000"/>
  <br>
  <em>Retail Scenario Data Generator + QA Framework</em>
</p>

<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-alpha-lightgrey">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.3.0-blueviolet">
</p>

---

# 🛒 Ecommerce Sales Database Generator

A YAML-configurable Python engine for generating synthetic, relational e-commerce databases — designed for SQL training, analytics storytelling, and realistic pipeline testing. This system goes beyond raw data: it simulates a full customer journey from browsing session to purchase, builds linked tables with referential integrity, and includes configurable messiness and built-in QA test suites to mirror real-world data challenges.
>📸 See it in action: [SQL Stories Portfolio Demo](https://github.com/G-Schumacher44/sql_stories_portfolio_demo)

___

## 🧩 TLDR;

- Generate synthetic, realistic e-commerce data (orders, returns, customers, etc.)
- **New:** Simulates cart abandonment, conversion rates, and repeat purchase behavior for realistic cohort analysis.
- YAML-controlled configuration of row volumes, faker behavior, return rates, etc.
- Plug-and-play messiness injection (via --messiness-level flag) for simulating real-world inconsistencies 
- Built-in QA tests: referential integrity, refund audits, return rate checks
- CLI runner, Pytest test suite, and optional big/mess audit extensions
- Designed for SQL project demos, portfolio datasets, and analytics onboarding


<details>
<summary> ⏯️ Quick Start</summary>

1. Clone the repository  
   ```bash
   git clone https://github.com/G-Schumacher44/ecom_sales_data_generator.git
   cd ecom_sales_data_generator
   # Install in editable mode
   pip install -e .
   ```

2. Run the CLI      
    ```bash
    ecomgen --config config/ecom_sales_gen_template.yaml --messiness-level none
    ```
</details>

---

## 📐 What’s Included

This project provides everything needed to simulate a realistic online retailer’s dataset for SQL, BI, or data science use:

- **Modular Generators**: Custom row generation logic for each core table (`orders`, `order_items`, `returns`, etc.)
- **YAML Config System**: Fine-grained control over generation volume, vocab, lookup tables, faker seed, and injection toggles
- **Messiness Engine**: Add typos, duplicates, nulls, formatting bugs, and numeric corruption
- **QA Framework**: Includes an automated Python suite (`qa_tests.py`) for validating data logic and a manual SQL script (`scripts/db_integrity_check.sql`) for direct database schema and integrity auditing.
- **CLI Interface**: One-command generation + validation from terminal or VS Code tasks
- **Editable Dev Mode**: Install via `pip install -e .` for active development and local CLI usage

### 📊 Database Overview

| Table Name        | Key Fields                                  | Purpose                                    |
| ----------------- | ------------------------------------------- | ------------------------------------------ |
| `orders`          | `order_id`, `customer_id`                   | Completed transactions and shipping costs  |
| `order_items`     | `order_item_id`, `order_id`, `product_id`   | Line-level product sales per order         |
| `returns`         | `return_id`, `order_id`, `return_reason`    | Return metadata per order                  |
| `return_items`    | `return_item_id`, `return_id`, `product_id` | Refunded products with values              |
| `shopping_carts`  | `cart_id`, `customer_id`, `status`          | Tracks cart activity (open, abandoned)     |
| `cart_items`      | `cart_item_id`, `cart_id`, `product_id`     | Products added to cart pre-purchase        |
| `product_catalog` | `product_id`, `product_name`, `unit_price`  | SKU definitions and margin proxy           |
| `customers`       | `customer_id`, `signup_date`                | Customer profiles and acquisition channels |

> 📌 View the full reference: [`database_schema_reference.md`](./database_schema_reference.md)


### 🧭 Orientation & Getting Started

<details>
<summary><strong>🧠 Notes from the Dev Team</strong></summary>
<br>

**Task and Purpose**

I built this system to reinforce, refresh, and evaluate my SQL skills through practical, repeatable analysis. Rather than relying on static datasets, I wanted something dynamic — a way to simulate the kinds of data challenges analysts face every day, with full control over volume, structure, and messiness.

**Why build a system and not just a script?**

This tool doesn’t just generate data — it builds a complete relational database simulation. I designed a system to go beyond isolated datasets by embedding referential logic, conditionally required fields, and analytical scenarios into the generation process.

You can see this engine in action in the [SQL Stories Portfolio Demo](https://github.com/G-Schumacher44/sql_stories_portfolio_demo), where I use AI-generated prompts to build SQL case studies end to end.

</details>

<details><summary><strong>📚 Resource Hub Links</strong></summary>
<br>

- [Generator Config Guide](./CONFIG_GUIDE.md)
- [Lake Config Guide](../datalakes_extention/CONFIG_GUIDE.md)
- [Testing Guide](../datalakes_extention/TESTING_GUIDE.md)
- [Database Schema Reference](./database_schema_reference.md)

</details>

---

## ▶️ Setup 

### 🔩 Configuration Setup

Use the YAML-based configuration system to control the size, structure, and messiness of your generated data.

<details>
<summary><strong>🧰 YAML Template</strong></summary>

- **File:** [`📝 ecom_sales_gen_template.yaml`](../../gen_config/ecom_sales_gen_template.yaml)
- **Purpose:** Defines how much data is generated, what kind of products are included, and the messiness level of the output.  
- **Use case:** Start here for most use cases. Adjust row counts, return rates, vocab, etc.

</details>

<details>
<summary><strong>📖 Full Config Guide</strong></summary>

- **File:** [`📘 CONFIG_GUIDE.md`](./CONFIG_GUIDE.md)
- **Purpose:** Explains how the YAML configuration works.  
- **Use case:** Perfect when you're creating your own custom scenario or tweaking advanced parameters.

</details>

### 📦 Dev Setup

Clone the repo and install in editable mode to enable local development:

```bash
# Clone repo and install in editable mode
git clone https://github.com/G-Schumacher44/ecom_sales_data_generator.git
cd ecom_sales_data_generator
pip install -e .
```

*Or set up the Conda environment:*

```bash
conda env create -f environment.yml
conda activate ecom_data_gen
```
___

### ▶️ CLI Usage

**Standard clean generation:**

```bash
ecomgen --config config/ecom_sales_gen_template.yaml --messiness-level baseline
```

___

## 🧪 Testing and Validation Guide

This project includes a comprehensive testing framework to ensure the integrity and quality of the synthetic data. Running these tests is highly recommended, especially after making changes to the configuration or generating new datasets.

For a detailed breakdown of each test suite, see the [**🧪 Testing and Validation Guide**](./TESTING_GUIDE.md).

<details>
<summary>🎯 Test Objectives</summary>

- **Config Integrity:** Ensure the YAML config is correctly structured and all required parameters are present.
- **Data Quality Rules:** Validate linkages (e.g., `order_id` in `returns` exists in `orders`), logic (e.g., refund ≤ order total), and schema expectations.
- **Messiness Audits:** Assess the applied messiness level (e.g., null injection, typos, formatting issues).

</details>

<details>
<summary>🛠️ Running the Tests</summary>

The two primary ways to test the system are:
1.  **Main QA Suite**: Runs automatically with the `ecomgen` command and validates the final data output.
2.  **Pytest Suite**: For developers to test the core logic in isolation.

- `test_config_integrity.py` – Confirms all required YAML fields exist
- `test_config_linting.py` – Lints YAML for structure and syntax
- `test_data_quality_rules.py` – Validates core business rules (e.g., referential integrity)

**Run them:**
```bash
pytest src/pytests/
```

</details>

___

## 🤝 On Generative AI Use

Generative AI tools (Gemini 2.5-PRO, ChatGPT 4o - 4.1) were used throughout this project as part of an integrated workflow — supporting code generation, documentation refinement, and idea testing. These tools accelerated development, but the logic, structure, and documentation reflect intentional, human-led design. This repository reflects a collaborative process: where automation supports clarity, and iteration deepens understanding.

---

## 📦 Licensing

This project is licensed under the [MIT License](../../LICENSE).

---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../datalakes_extention/CONFIG_GUIDE.md">⚙️ <b>Lake Config</b></a>
  &nbsp;·&nbsp;
  <a href="../datalakes_extention/CLI_REFERENCE.md">🧭 <b>CLI Reference</b></a>
  &nbsp;·&nbsp;
  <a href="../datalakes_extention/TESTING_GUIDE.md">🧪 <b>Testing</b></a>
  &nbsp;·&nbsp;
  <a href="../datalakes_extention/workflows/BACKLOG_BEAR.md">🧸 <b>Workflows</b></a>
  &nbsp;·&nbsp;
  <a href="./CONFIG_GUIDE.md">🛠️ <b>Generator Config</b></a>
</p>

<p align="center">
  <sub>✨ Synthetic Data · Python · QA Framework ✨</sub>
</p>
