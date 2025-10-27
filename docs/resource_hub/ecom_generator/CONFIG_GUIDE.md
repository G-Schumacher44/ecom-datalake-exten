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

# ⚙️ Configuration Guide

This guide explains how to structure and modify the YAML configuration file [`📝 ecom_sales_gen_template.yaml`](../../gen_config/ecom_sales_gen_template.yaml) to control the data generation process. Each section of the YAML allows you to fine-tune row counts, category distributions, vocabularies, and messiness parameters.

> ⬅️ [Back to Generator README](./README.md)

---

## 📚 Table of Contents

- [⚙️ Configuration Guide](#️-configuration-guide)
  - [📚 Table of Contents](#-table-of-contents)
  - [📁 Top-Level Sections](#-top-level-sections)
  - [📊 Key Simulation Parameters](#-key-simulation-parameters)
    - [Sales Funnel & Conversion](#sales-funnel--conversion)
    - [Customer Lifecycle & Behavioral Modeling](#customer-lifecycle--behavioral-modeling)
    - [Event-Driven & Seasonal Behavior](#event-driven--seasonal-behavior)
    - [Product & Profitability](#product--profitability)
    - [Operational Financials](#operational-financials)
    - [Earned Customer Value](#earned-customer-value)
    - [Returns & Refunds](#returns--refunds)
    - [Order & Channel Behavior](#order--channel-behavior)
  - [📋 Tables vs. Lookup Config](#-tables-vs-lookup-config)
  - [🧪 Experimenting](#-experimenting)

## 📁 Top-Level Sections

The YAML file is organized into several key sections:

- **`row_generators`**: Maps table names to the specific Python generator functions responsible for creating their data.
- **`output_dir`**: Specifies the directory where all generated CSV files and the SQL loader script will be saved.
- **`faker_seed`**: A seed value for the Faker library to ensure that generated data (like names and addresses) is reproducible across runs.
- **`tables`**: Defines the schema and generation rules for transactional tables like `shopping_carts`, `orders`, and `returns`.
- **`lookup_config`**: Defines the generation rules for foundational lookup tables like `customers` and `product_catalog`.
- **`vocab`**: Contains lists of controlled vocabulary used to populate categorical fields (e.g., `payment_methods`, `shipping_speeds`).
- **`parameters`**: A powerful section for controlling the core logic and statistical properties of the simulation.
- **`channel_rules`**: Allows for defining specific business rules that apply to different order channels (e.g., "Web" vs. "Phone").

---

## 📊 Key Simulation Parameters

The `parameters` section is where you control the most important aspects of the business simulation.

### Sales Funnel & Conversion

`order_days_back`: Defines the total duration of the simulation in days (e.g., 365 for one year). All generated events, from signups to orders and returns, will occur within this time window, ending on the current date.

`conversion_rate`: The baseline probability that a `shopping_cart` will be successfully converted into an `order`.

`first_purchase_conversion_boost`: A multiplier that increases the conversion rate for a customer's *first* potential purchase, based on their `signup_channel`. This simulates more effective onboarding for certain channels (e.g., `Phone`).

`time_to_first_cart_days_range`: Controls how many days after signing up a new customer will create their first shopping cart, simulating the initial engagement period.

`abandoned_cart_emptied_prob`: The probability that a cart that is not converted will be marked as `emptied` (with a total of 0 and no items) versus `abandoned` (with items remaining). This helps distinguish between passive abandonment and active disinterest.

### Customer Lifecycle & Behavioral Modeling

This is controlled by the `repeat_purchase_settings` block, which now supports highly stratified behavior.

`propensity_by_channel_and_tier`: This nested mapping defines the average number of repeat visits a customer will make. The simulation first looks for a rule matching the customer's `signup_channel`, then their `loyalty_tier`. This allows you to model scenarios where, for example, a "Gold" tier customer from "Phone" sales is more loyal than a "Gold" tier customer from "Social Media".

`time_delay_by_channel_and_tier`: This defines the time gap between customer visits. It's a nested mapping that specifies both a `range` (in days) and a `sigma` value. The `sigma` controls the variance of a log-normal distribution, allowing you to create "heavy tails" in the data (i.e., more realistic clusters of short and very long gaps between orders).

`cart_behavior_by_tier`: Controls the size and value of a customer's cart based on their `loyalty_tier`. You can define the `item_count_range` (how many different products) and `quantity_range` (how many of each product) to ensure that high-value customers place larger orders.

### Event-Driven & Seasonal Behavior

`seasonal_multipliers`: Defines month-by-month modifiers that scale the base order volume for each product category. This simulates peaks (e.g., holiday seasons) and troughs (off-season months).

`special_events`: Allows you to define date ranges for promotional events (like "Back to School" or "Cyber Week"). You can boost specific categories or channels during these windows.

### Product & Profitability

`product_margin_tiers`: Defines percent-based margins for each product category. This propagates into the financial metrics for order and return tables, keeping the dataset analytically consistent.

`bundle_probability`: Determines how often the generator creates "bundle" purchases — orders that include multiple related products.

### Operational Financials

`shipping_cost_range`: Controls the range of shipping costs applied to each order.

`payment_processing_fee_range`: Defines the transaction fee per order, allowing downstream financial analysis to mirror real expenses.

### Earned Customer Value

`customer_tier_thresholds`: Defines the total spend thresholds for each loyalty tier (e.g., Bronze, Silver, Gold). These are used when the generator calculates `loyalty_tier` and `clv_bucket`.

`tier_decay_rate`: Controls how fast customer tiers decay if a customer becomes inactive.

### Returns & Refunds

`return_rate`: Base probability that an order results in a return.

`return_reason_distribution`: Weighted distribution for return reasons (e.g., "Damaged", "Wrong Size").

`refund_adjustment_rules`: Adjusts refund amounts based on `return_reason`, allowing for partial refunds in certain scenarios.

`return_timing_distribution`: Creates a realistic long tail for returns. You can specify what percentage of returns occur within 30, 90, or 365 days.

`multi_return_probability`: The probability that an order that already has one return will have a second, separate return event, simulating more complex customer service scenarios.

### Order & Channel Behavior

`order_channel_distribution`: A weighted distribution for the channel of any given order.

`signup_channel_distribution`: A weighted distribution that controls how new customers are acquired (e.g., 55% from "Website", 15% from "Social Media"). This is foundational for modeling acquisition funnels.

`loyalty_distribution_by_channel`: Defines the probability of a new customer being assigned to an initial loyalty tier (`Bronze`, `Silver`, etc.) based on their `signup_channel`. This allows you to model scenarios where certain channels attract higher-value customers from the start.

`category_preference_by_signup_channel`: Skews the product categories a customer is likely to purchase from based on their `signup_channel`. For example, you can make "Social Media" customers more likely to buy "electronics" and "toys".

`channel_rules`: Defines channel-specific business logic, such as which `payment_methods` are allowed for "Web" vs. "Phone" orders.

---

## 📋 Tables vs. Lookup Config

It's important to understand the distinction between these two sections.

- **`lookup_config`**: Use this to generate your foundational, "lookup" tables. These are tables that other tables depend on, like `customers` and `product_catalog`. The generator runs this section first to create a cache of customers and products.
- **`tables`**: Use this for all other transactional tables that are generated *after* the lookups are created. This includes `shopping_carts`, `orders`, `returns`, and their corresponding item tables.

By separating these, the configuration remains logical and easy to follow.

---

## 🧪 Experimenting

The best way to learn is to experiment! Try changing the `conversion_rate` or the `propensity_by_tier` and see how it affects the final number of orders and the overall shape of your dataset.

---

<p align="center">
  <sub>✨ Synthetic Data · Python · QA Framework ✨</sub>
</p>

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
