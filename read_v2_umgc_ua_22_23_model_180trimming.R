#read_v2_umgc_ua_22_23_model_180trimming.R
# ============================================================
# DAACS Reading: 2PL item diagnostics and trimming workflow
# Working model: unidimensional 2PL
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(readr)
  library(tibble)
  library(purrr)
  library(ggplot2)
  library(mirt)
})

# ------------------------------------------------------------
# 0. Configuration
# ------------------------------------------------------------

output_dir <- "C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_model_180trimming_outputs"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------
# 1. Load final dataset and fitted 2PL model
# ------------------------------------------------------------

read_v2_umgc_ua_22_23_final <- readRDS("C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_final_outputs/read_v2_umgc_ua_22_23_final.rds")
mod_2pl <- readRDS(file.path("C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_model_comparison_outputs/mod_2pl_read180.rds"))

dat <- read_v2_umgc_ua_22_23_final
item_cols <- grep("^Q\\d{3}", names(dat), value = TRUE)

stopifnot(length(item_cols) == 180)

read_items <- dat %>%
  select(all_of(item_cols)) %>%
  mutate(across(everything(), ~ as.integer(.x)))

# ------------------------------------------------------------
# 2. Item lookup from item names
# ------------------------------------------------------------

item_lookup <- tibble(QID = item_cols) %>%
  mutate(
    testlet = str_extract(QID, "\\d+$"),
    domain_code = str_match(QID, "^Q\\d{3}([A-Za-z]+)\\d+$")[, 2],
    domain = case_when(
      domain_code == "s"  ~ "structure",
      domain_code == "in" ~ "inference",
      domain_code == "id" ~ "ideas",
      domain_code == "p"  ~ "purpose",
      domain_code == "l"  ~ "language",
      TRUE ~ NA_character_
    )
  )

# ------------------------------------------------------------
# 3. Item response counts
# ------------------------------------------------------------

item_counts <- tibble(
  QID = item_cols,
  n_responded = sapply(read_items[item_cols], function(x) sum(!is.na(x))),
  prop_correct = sapply(read_items[item_cols], function(x) mean(x == 1, na.rm = TRUE)),
  prop_wrong = sapply(read_items[item_cols], function(x) mean(x == 0, na.rm = TRUE))
) %>%
  arrange(QID)

# ------------------------------------------------------------
# 4. Extract 2PL item parameters
# ------------------------------------------------------------

coef_2pl <- coef(mod_2pl, IRTpars = TRUE, simplify = TRUE)

item_pars_2pl <- as.data.frame(coef_2pl$items) %>%
  tibble::rownames_to_column("QID")

# Keep only columns that actually exist
# For dichotomous 2PL in mirt with IRTpars=TRUE, expect a and b
item_pars_2pl <- item_pars_2pl %>%
  transmute(
    QID,
    a_2pl = if ("a" %in% names(.)) a else NA_real_,
    b_2pl = if ("b" %in% names(.)) b else NA_real_,
    g_2pl = if ("g" %in% names(.)) g else NA_real_,
    u_2pl = if ("u" %in% names(.)) u else NA_real_
  )

# ------------------------------------------------------------
# 5. Build item diagnostics table
# ------------------------------------------------------------

item_diagnostics_2pl <- item_lookup %>%
  left_join(item_counts, by = "QID") %>%
  left_join(item_pars_2pl, by = "QID") %>%
  mutate(
    flag_low_a = !is.na(a_2pl) & a_2pl < 0.40,
    flag_high_a = !is.na(a_2pl) & a_2pl > 3.00,
    flag_extreme_b = !is.na(b_2pl) & (b_2pl < -3.50 | b_2pl > 3.50),
    flag_low_n = !is.na(n_responded) & n_responded < 500,
    flag_poor_p = !is.na(prop_correct) & (prop_correct < 0.05 | prop_correct > 0.95),
    n_flags = rowSums(across(
      c(flag_low_a, flag_high_a, flag_extreme_b, flag_low_n, flag_poor_p),
      ~ as.integer(.x)
    ), na.rm = TRUE),
    trim_candidate = n_flags > 0
  ) %>%
  arrange(desc(trim_candidate), desc(n_flags), a_2pl, desc(abs(b_2pl)))

write_csv(
  item_diagnostics_2pl,
  file.path(output_dir, "item_diagnostics_2pl_read180.csv")
)

print(item_diagnostics_2pl, n = 30)

# ------------------------------------------------------------
# 6. Summary table of flagged items
# ------------------------------------------------------------

flag_summary_2pl <- tibble(
  criterion = c(
    "low_discrimination_a_lt_0.40",
    "high_discrimination_a_gt_3.00",
    "extreme_difficulty_abs_b_gt_3.50",
    "low_response_count_n_lt_500",
    "extreme_pvalue_prop_correct_lt_0.05_or_gt_0.95",
    "any_flag"
  ),
  n_items = c(
    sum(item_diagnostics_2pl$flag_low_a, na.rm = TRUE),
    sum(item_diagnostics_2pl$flag_high_a, na.rm = TRUE),
    sum(item_diagnostics_2pl$flag_extreme_b, na.rm = TRUE),
    sum(item_diagnostics_2pl$flag_low_n, na.rm = TRUE),
    sum(item_diagnostics_2pl$flag_poor_p, na.rm = TRUE),
    sum(item_diagnostics_2pl$trim_candidate, na.rm = TRUE)
  )
)

write_csv(
  flag_summary_2pl,
  file.path(output_dir, "item_diagnostics_2pl_flag_summary.csv")
)

print(flag_summary_2pl)

# ------------------------------------------------------------
# 7. Quick plots for review
# ------------------------------------------------------------

p_a <- ggplot(item_diagnostics_2pl, aes(x = a_2pl)) +
  geom_histogram(bins = 30) +
  geom_vline(xintercept = c(0.40, 3.00), linetype = "dashed") +
  labs(title = "2PL discrimination parameters", x = "a", y = "Count") +
  theme_minimal()

p_b <- ggplot(item_diagnostics_2pl, aes(x = b_2pl)) +
  geom_histogram(bins = 30) +
  geom_vline(xintercept = c(-3.50, 3.50), linetype = "dashed") +
  labs(title = "2PL difficulty parameters", x = "b", y = "Count") +
  theme_minimal()

p_ab <- ggplot(item_diagnostics_2pl, aes(x = b_2pl, y = a_2pl, label = QID)) +
  geom_point(alpha = 0.7) +
  labs(title = "2PL item parameter map", x = "b", y = "a") +
  theme_minimal()

ggsave(file.path(output_dir, "item_diagnostics_2pl_hist_a.png"), p_a, width = 7, height = 5, dpi = 300)
ggsave(file.path(output_dir, "item_diagnostics_2pl_hist_b.png"), p_b, width = 7, height = 5, dpi = 300)
ggsave(file.path(output_dir, "item_diagnostics_2pl_scatter_ab.png"), p_ab, width = 7, height = 5, dpi = 300)

# ------------------------------------------------------------
# 8. Create reviewed trim table
# IMPORTANT:
# This creates a first-pass auto-flagged table.
# You should inspect and optionally edit the 'drop_item' column manually.
# ------------------------------------------------------------

trim_review_table <- item_diagnostics_2pl %>%
  mutate(
    drop_item = trim_candidate,
    reviewer_note = NA_character_
  )

write_csv(
  trim_review_table,
  file.path(output_dir, "item_trim_review_table_read180.csv")
)

# ------------------------------------------------------------
# 9. Build trimmed item pool
# Option A: immediate auto-trim from current rules
# Option B: after manual review, read edited CSV back in
# ------------------------------------------------------------

# ---- Option A: automatic trimming directly from current flags ----
trimmed_items_auto <- trim_review_table %>%
  filter(!drop_item) %>%
  pull(QID)

# ---- Option B: after you manually inspect/edit the review file ----
# reviewed_trim <- read_csv(file.path(output_dir, "item_trim_review_table_read180_reviewed.csv"),
#                           show_col_types = FALSE)
# trimmed_items_final <- reviewed_trim %>%
#   filter(!drop_item) %>%
#   pull(QID)

# For now, use automatic version
trimmed_items_final <- trimmed_items_auto

length(trimmed_items_final)

write_csv(
  tibble(QID = trimmed_items_final),
  file.path(output_dir, "trimmed_item_pool_read.csv")
)

write_csv(
  tibble(QID = setdiff(item_cols, trimmed_items_final)),
  file.path(output_dir, "dropped_item_pool_read.csv")
)

# ------------------------------------------------------------
# 10. Refit 2PL on trimmed pool
# ------------------------------------------------------------

read_items_trimmed <- read_items %>%
  select(all_of(trimmed_items_final))

set.seed(2026)

mod_2pl_trimmed <- mirt(
  data = read_items_trimmed,
  model = 1,
  itemtype = "2PL",
  verbose = TRUE
)

saveRDS(
  mod_2pl_trimmed,
  file.path(output_dir, "mod_2pl_trimmed_read.rds")
)

# ------------------------------------------------------------
# 11. Compare full vs trimmed 2PL model fit
# ------------------------------------------------------------

fit_full_vs_trimmed <- tibble(
  model = c("2PL_full_180", "2PL_trimmed"),
  n_items = c(length(item_cols), length(trimmed_items_final)),
  npar = c(
    tryCatch(sum(mod2values(mod_2pl)$est), error = function(e) NA_real_),
    tryCatch(sum(mod2values(mod_2pl_trimmed)$est), error = function(e) NA_real_)
  ),
  logLik = c(
    tryCatch(mod_2pl@Fit$logLik, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$logLik, error = function(e) NA_real_)
  ),
  AIC = c(
    tryCatch(mod_2pl@Fit$AIC, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$AIC, error = function(e) NA_real_)
  ),
  SABIC = c(
    tryCatch(mod_2pl@Fit$SABIC, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$SABIC, error = function(e) NA_real_)
  ),
  HQ = c(
    tryCatch(mod_2pl@Fit$HQ, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$HQ, error = function(e) NA_real_)
  ),
  BIC = c(
    tryCatch(mod_2pl@Fit$BIC, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$BIC, error = function(e) NA_real_)
  )
)

write_csv(
  fit_full_vs_trimmed,
  file.path(output_dir, "fit_full_vs_trimmed_2pl_read.csv")
)

print(fit_full_vs_trimmed)

# ------------------------------------------------------------
# 12. Extract theta scores: full vs trimmed
# ------------------------------------------------------------

theta_full <- fscores(
  mod_2pl,
  method = "EAP",
  full.scores = TRUE,
  full.scores.SE = TRUE
) %>%
  as.data.frame()

names(theta_full)[1:2] <- c("theta_2pl_full", "SE_theta_2pl_full")

theta_trimmed <- fscores(
  mod_2pl_trimmed,
  method = "EAP",
  full.scores = TRUE,
  full.scores.SE = TRUE
) %>%
  as.data.frame()

names(theta_trimmed)[1:2] <- c("theta_2pl_trimmed", "SE_theta_2pl_trimmed")

theta_compare_trimmed <- dat %>%
  select(
    DAACS_ID, global_id, college, wave, age,
    gender, ethnicity, military, pell, transfer,
    readTime
  ) %>%
  bind_cols(theta_full, theta_trimmed) %>%
  mutate(
    diff_full_trimmed = theta_2pl_full - theta_2pl_trimmed,
    absdiff_full_trimmed = abs(diff_full_trimmed),
    avg_full_trimmed = (theta_2pl_full + theta_2pl_trimmed) / 2
  )

write_csv(
  theta_compare_trimmed,
  file.path(output_dir, "theta_compare_full_vs_trimmed_2pl_read.csv")
)

# ------------------------------------------------------------
# 13. Summary of theta changes after trimming
# ------------------------------------------------------------

theta_trimmed_summary <- tibble(
  correlation_full_trimmed = cor(
    theta_compare_trimmed$theta_2pl_full,
    theta_compare_trimmed$theta_2pl_trimmed,
    use = "pairwise.complete.obs"
  ),
  mean_absdiff = mean(theta_compare_trimmed$absdiff_full_trimmed, na.rm = TRUE),
  median_absdiff = median(theta_compare_trimmed$absdiff_full_trimmed, na.rm = TRUE),
  p90_absdiff = quantile(theta_compare_trimmed$absdiff_full_trimmed, 0.90, na.rm = TRUE),
  p95_absdiff = quantile(theta_compare_trimmed$absdiff_full_trimmed, 0.95, na.rm = TRUE)
)

write_csv(
  theta_trimmed_summary,
  file.path(output_dir, "theta_trimmed_summary_read.csv")
)

print(theta_trimmed_summary)

# ------------------------------------------------------------
# 14. Plot full vs trimmed theta comparison
# ------------------------------------------------------------

p_full_trimmed_scatter <- ggplot(
  theta_compare_trimmed,
  aes(x = theta_2pl_full, y = theta_2pl_trimmed)
) +
  geom_point(alpha = 0.25) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Full vs Trimmed 2PL Theta",
    x = "2PL theta (full 180-item pool)",
    y = "2PL theta (trimmed pool)"
  ) +
  theme_minimal()

p_full_trimmed_diffavg <- ggplot(
  theta_compare_trimmed,
  aes(x = avg_full_trimmed, y = diff_full_trimmed)
) +
  geom_point(alpha = 0.25) +
  geom_hline(yintercept = mean(theta_compare_trimmed$diff_full_trimmed, na.rm = TRUE),
             linetype = "dashed") +
  labs(
    title = "Difference vs Average: Full 2PL - Trimmed 2PL",
    x = "Average theta",
    y = "Difference"
  ) +
  theme_minimal()

ggsave(file.path(output_dir, "scatter_full_vs_trimmed_2pl.png"),
       p_full_trimmed_scatter, width = 7, height = 5, dpi = 300)

ggsave(file.path(output_dir, "diffavg_full_vs_trimmed_2pl.png"),
       p_full_trimmed_diffavg, width = 7, height = 5, dpi = 300)

# ------------------------------------------------------------
# 15. Optional subgroup summaries of trimming impact
# ------------------------------------------------------------

group_vars <- c("college", "wave", "gender", "ethnicity", "military", "pell")

theta_trimmed_by_group <- purrr::map_dfr(group_vars, function(v) {
  theta_compare_trimmed %>%
    mutate(group_value = as.character(.data[[v]])) %>%
    group_by(grouping_variable = v, group_value) %>%
    summarise(
      n = n(),
      mean_diff = mean(diff_full_trimmed, na.rm = TRUE),
      mean_absdiff = mean(absdiff_full_trimmed, na.rm = TRUE),
      .groups = "drop"
    )
})

write_csv(
  theta_trimmed_by_group,
  file.path(output_dir, "theta_trimmed_by_group_read.csv")
)

theta_trimmed_continuous <- tibble(
  predictor = c("age", "transfer"),
  cor_absdiff = c(
    cor(theta_compare_trimmed$age, theta_compare_trimmed$absdiff_full_trimmed,
        use = "pairwise.complete.obs"),
    cor(theta_compare_trimmed$transfer, theta_compare_trimmed$absdiff_full_trimmed,
        use = "pairwise.complete.obs")
  )
)

write_csv(
  theta_trimmed_continuous,
  file.path(output_dir, "theta_trimmed_continuous_read.csv")
)

cat("\n2PL item-diagnostics and trimming workflow finished.\n")
cat("Original items:", length(item_cols), "\n")
cat("Trimmed items retained:", length(trimmed_items_final), "\n")
cat("Items dropped:", length(setdiff(item_cols, trimmed_items_final)), "\n")